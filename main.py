import socket
import json
import threading
import select
import time
import sys
import os
import logging
import queue  # Para JSON asíncrono
from datetime import datetime

# --- Configuración ---
LISTEN_IP = "0.0.0.0"
TRACCAR_HOST = "localhost"
JSON_FORWARD_HOST = "localhost"
JSON_FORWARD_PORT = 7005
OMIT_FILENAME = "omit_list.txt"

DEVICE_PORTS_MAP = {
    6013: 5013,  # Sinotrack (UDP/TCP)
    6001: 5001,  # Coban (TCP/UDP - aunque más común TCP)
    6027: 5027,  # Teltonika (TCP)
}

# --- Gestión de Omisión ---
omit_identifiers = set()
omit_lock = threading.Lock()

# --- Constantes y Colas ---
BUFFER_SIZE = 4096
SOCKET_TIMEOUT = 180  # TCP inactivity timeout
UDP_TIMEOUT = 300  # UDP Relay inactivity timeout
CLEANUP_INTERVAL = 60  # UDP Cleanup check interval
JSON_QUEUE = queue.Queue(maxsize=10000)  # Cola para enviar a JSON de forma asíncrona
# maxsize previene consumo excesivo de memoria si el receptor falla

# --- Estructuras de Datos para UDP con Estado ---
active_udp_relays = (
    {}
)  # relay_socket -> (client_address, last_activity, listening_socket)
client_to_relay_socket = {}  # client_address -> relay_socket
relay_lock = (
    threading.Lock()
)  # Protege TODAS las estructuras UDP y monitored_sockets (para relays)
monitored_sockets = []  # Lista global para select (sockets de escucha + relays UDP)


# --- Configuración del Logging ---
def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(
        log_dir, f"log-{datetime.now().strftime('%Y-%m-%d')}.log"
    )
    logging.basicConfig(
        level=logging.DEBUG,  # Captura todo
        format="%(asctime)s [%(levelname)s] {%(threadName)s} %(message)s",  # Incluir nombre del hilo
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    # Silenciar logs muy verbosos de librerías si es necesario
    # logging.getLogger("some_library").setLevel(logging.WARNING)
    logging.info("Logging configurado. Nivel: DEBUG. Escribiendo a: %s", log_filename)


# --- Cargar Identificadores a Omitir ---
def load_omitted_identifiers(filename=OMIT_FILENAME):
    loaded_ids = set()
    try:
        with open(filename, "r", encoding="utf-8") as f:
            loaded_ids = {line.strip() for line in f if line.strip()}
        if loaded_ids:
            with omit_lock:
                omit_identifiers.clear()
                omit_identifiers.update(loaded_ids)
            logging.info(
                f"Cargados {len(loaded_ids)} identificadores para omitir desde {filename}"
            )
        else:
            logging.info(f"Archivo de omisión {filename} vacío o sin IDs válidos.")
            with omit_lock:
                omit_identifiers.clear()
    except FileNotFoundError:
        logging.info(f"Archivo de omisión {filename} no encontrado.")
        with omit_lock:
            omit_identifiers.clear()
    except Exception as e:
        logging.error(f"Error cargando archivo de omisión {filename}: {e}")
        with omit_lock:
            omit_identifiers.clear()


# --- Función para verificar si se debe omitir (con condición 'tracker') ---
def should_omit(data):
    try:
        decoded_for_check = data.decode("utf-8", errors="ignore")
        identifier_found_in_message = None
        with omit_lock:
            if not omit_identifiers:
                return False
            for identifier in omit_identifiers:
                if identifier in decoded_for_check:
                    identifier_found_in_message = identifier
                    break
        if identifier_found_in_message:
            # Verificar si 'tracker' está presente (ignorar mayúsculas/minúsculas)
            if "tracker" in decoded_for_check.lower():
                logging.info(
                    f"Omitiendo mensaje con 'tracker' del ID omitido: {identifier_found_in_message}"
                )
                return True
            else:
                logging.debug(
                    f"Mensaje de ID omitido {identifier_found_in_message} permitido (sin 'tracker')."
                )
                return False
        else:
            return False
    except UnicodeDecodeError:
        logging.debug("No se pudo decodificar UTF-8 para verificar omisión.")
        return False
    except Exception as e:
        logging.error(f"Error inesperado en should_omit: {e}")
        return False


# --- Función para poner datos en la Cola JSON ---
def forward_to_json_port(listen_port, data, source_address, protocol="TCP"):
    """Prepara el payload EXACTO requerido y lo pone en la cola JSON."""
    # Nota: 'protocol', 'source_ip', 'source_port', 'traccar_port' ya no son necesarios
    # para el payload final, pero los mantenemos por si son útiles en el futuro o para logs aquí.
    try:
        # El payload para la cola SÓLO necesita lo esencial para reconstruir el JSON final
        payload_for_queue = {
            "port": listen_port,  # Puerto de escucha ORIGINAL del proxy
            "raw_data": data,  # Los bytes crudos recibidos del dispositivo
            # Podríamos añadir source_address para logging en el sender si es útil
            "source_address_for_log": source_address,
        }
        JSON_QUEUE.put(payload_for_queue, block=False)
        logging.debug(
            f"-> JSON QUEUE: Datos de {protocol} {source_address} (Puerto {listen_port}) encolados."
        )
    except queue.Full:
        data_preview = data[:50].hex() + "..."  # Vista previa segura
        logging.warning(
            f"Cola JSON llena! Descartando datos de {protocol} {source_address}. Preview(hex): {data_preview}"
        )
    except Exception as e:
        logging.error(f"Error inesperado al encolar para JSON: {e}")


# --- Hilo Sender para la Cola JSON ---
def json_sender_thread_func():
    """Toma datos de la cola, genera el JSON en el formato correcto y lo envía."""
    while True:
        json_socket = None
        payload_from_queue = None
        try:
            payload_from_queue = JSON_QUEUE.get()  # Espera por un item
            listen_port = payload_from_queue["port"]
            raw_data = payload_from_queue["raw_data"]
            source_addr_log = payload_from_queue.get(
                "source_address_for_log", ("?", "?")
            )  # Para logs

            # Decodificar los datos crudos para el campo 'data' del JSON
            try:
                decoded_data = raw_data.decode("utf-8", errors="replace")
            except UnicodeDecodeError:
                # Si falla UTF-8, ¿qué espera tu receptor? ¿Hex? ¿Base64?
                # Asumamos que prefiere UTF-8 con reemplazo por ahora.
                # Si necesitas Hex: decoded_data = raw_data.hex()
                # Si necesitas Base64: import base64; decoded_data = base64.b64encode(raw_data).decode('ascii')
                pass  # Ya está decodificado con reemplazo

            # Crear el diccionario JSON FINAL con las claves correctas
            final_json_payload = {
                "port": listen_port,
                "data": decoded_data,
                "timestamp": time.time(),
            }

            # Convertir a JSON string y codificar a bytes
            json_string = json.dumps(final_json_payload)
            encoded_json = json_string.encode("utf-8")

            # Conectar y enviar
            json_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            json_socket.settimeout(5)
            json_socket.connect((JSON_FORWARD_HOST, JSON_FORWARD_PORT))
            json_socket.sendall(encoded_json)
            logging.debug(
                f"JSON SENDER: Datos enviados para puerto {listen_port} (originado por {source_addr_log})"
            )
            JSON_QUEUE.task_done()

        except KeyError as e:
            logging.error(
                f"JSON SENDER: Error de clave procesando payload de la cola: {e} - Payload: {payload_from_queue}"
            )
            if payload_from_queue:
                JSON_QUEUE.task_done()  # Marcar como hecho para no bloquear
        except socket.timeout:
            logging.error(
                f"JSON SENDER: Timeout conectando/enviando a {JSON_FORWARD_HOST}:{JSON_FORWARD_PORT}"
            )
            if payload_from_queue:
                JSON_QUEUE.task_done()
        except socket.error as e:
            logging.error(
                f"JSON SENDER: Error de socket con {JSON_FORWARD_HOST}:{JSON_FORWARD_PORT}: {e}"
            )
            if payload_from_queue:
                JSON_QUEUE.task_done()
        except Exception as e:
            logging.error(
                f"JSON SENDER: Error inesperado: {e}", exc_info=True
            )  # Loggear traceback
            if payload_from_queue:
                JSON_QUEUE.task_done()
        finally:
            if json_socket:
                try:
                    json_socket.close()
                except socket.error:
                    pass
            if payload_from_queue is None:
                time.sleep(0.1)  # Pausa si la cola estaba vacía


# --- Manejador para Conexiones TCP ---
def handle_tcp_client(client_socket, client_address, listen_port):
    # (Lógica casi idéntica, solo llama a la nueva forward_to_json_port que usa la cola)
    traccar_port = DEVICE_PORTS_MAP.get(listen_port)
    if not traccar_port:
        logging.error(f"TCP: No se encontró mapeo de puerto Traccar para {listen_port}")
        client_socket.close()
        return

    logging.info(
        f"TCP: Conexión de {client_address} puerto {listen_port} -> Traccar {TRACCAR_HOST}:{traccar_port}"
    )
    traccar_socket = None
    connection_active_time = time.time()
    try:
        traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Opciones de socket para posible mejora de latencia (experimentar)
        # traccar_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        traccar_socket.settimeout(10)
        traccar_socket.connect((TRACCAR_HOST, traccar_port))
        traccar_socket.settimeout(SOCKET_TIMEOUT)
        client_socket.settimeout(SOCKET_TIMEOUT)
        logging.debug(f"TCP: Conexión establecida con Traccar para {client_address}")
        sockets = [client_socket, traccar_socket]
        running = True
        while running:
            try:
                readable, _, exceptional = select.select(
                    sockets, [], sockets, SOCKET_TIMEOUT
                )
                if not readable and not exceptional:
                    logging.info(
                        f"TCP: Timeout ({SOCKET_TIMEOUT}s) para {client_address}. Cerrando."
                    )
                    running = False
                    break
                for sock in exceptional:
                    logging.error(
                        f"TCP: Error excepcional ({'cliente' if sock == client_socket else 'Traccar'}) para {client_address}. Cerrando."
                    )
                    running = False
                    break  # Salir del bucle interno de sockets
                if not running:
                    break  # Salir del while principal si hubo error excepcional
                for sock in readable:
                    data = sock.recv(BUFFER_SIZE)
                    if not data:
                        logging.info(
                            f"TCP: Conexión cerrada por {'cliente' if sock == client_socket else 'Traccar'} para {client_address}. Duración: {time.time() - connection_active_time:.2f}s."
                        )
                        running = False
                        break
                    if sock == client_socket:
                        logging.debug(
                            f"TCP: Cliente {client_address} -> Traccar ({len(data)} bytes)"
                        )
                        if should_omit(data):
                            logging.debug(
                                f"TCP: Datos de Cliente {client_address} OMITIDOS."
                            )
                            continue
                        traccar_socket.sendall(data)
                        forward_to_json_port(
                            listen_port, data, client_address, "TCP"
                        )  # Encola
                    else:  # sock == traccar_socket
                        logging.debug(
                            f"TCP: Traccar -> Cliente {client_address} ({len(data)} bytes)"
                        )
                        client_socket.sendall(data)
            except socket.timeout:
                logging.info(
                    f"TCP: Socket timeout inesperado para {client_address}. Cerrando."
                )
                running = False
            except socket.error as e:
                logging.error(
                    f"TCP: Error socket recv/send para {client_address}: {e}. Cerrando."
                )
                running = False
            except Exception as e:
                logging.error(
                    f"TCP: Error inesperado manejando {client_address}: {e}. Cerrando."
                )
                running = False
            if not running:
                break
    except socket.timeout:
        logging.error(
            f"TCP: Timeout conectando a Traccar {TRACCAR_HOST}:{traccar_port} para {client_address}"
        )
    except socket.error as e:
        logging.error(
            f"TCP: Error socket (conexión/inicial) {client_address} -> Traccar: {e}"
        )
    except Exception as e:
        logging.error(
            f"TCP: Error general configurando conexión para {client_address}: {e}"
        )
    finally:
        logging.debug(f"TCP: Limpiando conexión para {client_address}")
        # ... (cierre robusto de sockets igual que antes) ...
        if client_socket:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except:
                pass
            try:
                client_socket.close()
            except:
                pass
        if traccar_socket:
            try:
                traccar_socket.shutdown(socket.SHUT_RDWR)
            except:
                pass
            try:
                traccar_socket.close()
            except:
                pass
        logging.info(
            f"TCP: Conexión finalizada para {client_address} puerto {listen_port}. Duración: {time.time() - connection_active_time:.2f}s"
        )


# --- NUEVA FUNCIÓN: Manejador para Paquetes UDP (Cliente -> Proxy) ---
def handle_udp_from_client(listening_socket, data, client_address):
    """Procesa un paquete UDP recibido de un dispositivo. Busca/crea relay y reenvía."""
    global monitored_sockets, active_udp_relays, client_to_relay_socket, relay_lock
    listen_port = listening_socket.getsockname()[1]
    traccar_port = DEVICE_PORTS_MAP.get(listen_port)
    if not traccar_port:
        logging.warning(
            f"UDP: Puerto {listen_port} sin mapeo para {client_address}. Ignorando."
        )
        return

    logging.debug(
        f"UDP: IN <= Cliente {client_address} puerto {listen_port} ({len(data)} bytes)"
    )
    if should_omit(data):
        logging.debug(f"UDP: Datos de {client_address} OMITIDOS.")
        return

    forward_to_json_port(listen_port, data, client_address, "UDP")  # Encola

    relay_socket_to_use = None
    # === Inicio Sección Crítica UDP ===
    with relay_lock:
        now = time.time()
        if client_address in client_to_relay_socket:  # Buscar relay existente
            existing_relay_socket = client_to_relay_socket[client_address]
            if existing_relay_socket in active_udp_relays:  # Verificar que aún existe
                _, _, listening_sock_ref = active_udp_relays[existing_relay_socket]
                active_udp_relays[existing_relay_socket] = (
                    client_address,
                    now,
                    listening_sock_ref,
                )  # Actualizar timestamp
                relay_socket_to_use = existing_relay_socket
                logging.debug(
                    f"UDP: Usando relay existente {existing_relay_socket.getsockname()} para {client_address}"
                )
            else:  # Limpiar mapeo inconsistente
                del client_to_relay_socket[client_address]
                logging.warning(
                    f"UDP: Mapeo de relay obsoleto limpiado para {client_address}"
                )

        if (
            relay_socket_to_use is None
        ):  # Crear nuevo relay si no se encontró/estaba obsoleto
            logging.debug(f"UDP: Creando NUEVO relay para {client_address}")
            try:
                relay_socket_to_use = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                relay_socket_to_use.setblocking(False)
                relay_socket_to_use.bind((LISTEN_IP, 0))  # Puerto efímero
                # Añadir a estructuras bajo el mismo lock
                monitored_sockets.append(relay_socket_to_use)
                active_udp_relays[relay_socket_to_use] = (
                    client_address,
                    now,
                    listening_socket,
                )
                client_to_relay_socket[client_address] = relay_socket_to_use
                logging.info(
                    f"UDP: Nuevo relay {relay_socket_to_use.getsockname()} creado para {client_address}. Total relays: {len(active_udp_relays)}"
                )
            except Exception as e:
                logging.error(
                    f"UDP: Fallo al crear socket relay para {client_address}: {e}"
                )
                if relay_socket_to_use:  # Limpiar si se creó parcialmente
                    if relay_socket_to_use in monitored_sockets:
                        monitored_sockets.remove(relay_socket_to_use)
                    try:
                        relay_socket_to_use.close()
                    except:
                        pass
                relay_socket_to_use = None  # Indicar fallo
    # === Fin Sección Crítica UDP ===

    if relay_socket_to_use:  # Enviar a Traccar si tenemos un relay válido
        try:
            relay_socket_to_use.sendto(data, (TRACCAR_HOST, traccar_port))
            logging.debug(
                f"UDP: OUT => Traccar {TRACCAR_HOST}:{traccar_port} via {relay_socket_to_use.getsockname()} para {client_address}"
            )
        except socket.error as e:
            logging.error(
                f"UDP: Error enviando a Traccar para {client_address} via {relay_socket_to_use.getsockname()}: {e}"
            )
            # Podríamos considerar invalidar/limpiar el relay aquí si el error es persistente (ej. Network Unreachable)
            # pero por ahora solo loggeamos.


# --- Limpieza Periódica de Relays UDP Inactivos ---
def cleanup_inactive_udp_relays():
    global monitored_sockets, active_udp_relays, client_to_relay_socket, relay_lock
    while True:
        time.sleep(CLEANUP_INTERVAL)
        now = time.time()
        cleaned_count = 0
        sockets_to_cleanup = []  # Guardar sockets a cerrar/eliminar

        # === Inicio Sección Crítica UDP ===
        with relay_lock:
            # Iterar sobre una copia de las keys para poder modificar el diccionario
            current_relays = list(active_udp_relays.keys())
            for relay_socket in current_relays:
                if (
                    relay_socket in active_udp_relays
                ):  # Doble check por si fue eliminado entretanto
                    client_address, last_activity, _ = active_udp_relays[relay_socket]
                    if now - last_activity > UDP_TIMEOUT:
                        sockets_to_cleanup.append((relay_socket, client_address))
                        # Eliminar de las estructuras AHORA DENTRO DEL LOCK
                        del active_udp_relays[relay_socket]
                        if client_address in client_to_relay_socket:
                            del client_to_relay_socket[client_address]
                        # Eliminar de monitored_sockets AHORA DENTRO DEL LOCK
                        if relay_socket in monitored_sockets:
                            monitored_sockets.remove(relay_socket)
                        else:
                            logging.warning(
                                f"UDP Cleanup: Socket {relay_socket.getsockname() if hasattr(relay_socket,'getsockname') else '?'} no encontrado en monitored_sockets durante limpieza."
                            )
                        cleaned_count += 1

        # === Fin Sección Crítica UDP ===

        # Cerrar los sockets fuera del lock principal
        for relay_socket, client_address in sockets_to_cleanup:
            relay_info = f"relay {relay_socket.getsockname() if hasattr(relay_socket,'getsockname') else '?'} de {client_address}"
            logging.info(
                f"UDP: Timeout para {relay_info}. Limpiando (cerrando socket)."
            )
            try:
                relay_socket.close()
            except Exception as e:
                logging.error(
                    f"UDP: Error cerrando socket durante limpieza de {relay_info}: {e}"
                )

        if cleaned_count > 0:
            logging.info(
                f"UDP: Limpieza completada. {cleaned_count} relays inactivos eliminados."
            )
        with relay_lock:  # Obtener valores actuales para loggear
            active_count = len(active_udp_relays)
            monitored_count = len(monitored_sockets)
        logging.debug(
            f"UDP: Relays activos: {active_count}. Sockets Monitoreados: {monitored_count}"
        )


# --- Servidor Principal ---
def start_server():
    global monitored_sockets, active_udp_relays, client_to_relay_socket, relay_lock
    server_sockets = []  # Sockets de escucha
    udp_listening_sockets = {}  # listen_port -> udp_listening_socket

    load_omitted_identifiers()  # Cargar IDs

    # Iniciar hilo sender JSON
    json_sender = threading.Thread(
        target=json_sender_thread_func, name="JsonSenderThread", daemon=True
    )
    json_sender.start()
    logging.info("Hilo sender JSON iniciado.")

    # Crear sockets de escucha
    for listen_port, traccar_port in DEVICE_PORTS_MAP.items():
        # TCP
        try:
            tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            tcp_sock.bind((LISTEN_IP, listen_port))
            tcp_sock.listen(128)
            tcp_sock.setblocking(False)
            server_sockets.append(tcp_sock)
            # No necesitamos lock aquí porque es antes de que otros hilos accedan
            monitored_sockets.append(tcp_sock)
            logging.info(
                f"Escuchando TCP en {LISTEN_IP}:{listen_port} -> Traccar {TRACCAR_HOST}:{traccar_port}"
            )
        except OSError as e:
            logging.critical(f"Fallo escucha TCP {listen_port}: {e}. Saliendo.")
            exit(1)
        # UDP
        try:
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            udp_sock.bind((LISTEN_IP, listen_port))
            udp_sock.setblocking(False)
            server_sockets.append(udp_sock)
            # No necesitamos lock aquí
            monitored_sockets.append(udp_sock)
            udp_listening_sockets[listen_port] = udp_sock
            logging.info(
                f"Escuchando UDP en {LISTEN_IP}:{listen_port} -> Traccar {TRACCAR_HOST}:{traccar_port}"
            )
        except OSError as e:
            logging.critical(f"Fallo escucha UDP {listen_port}: {e}. Saliendo.")
            exit(1)

    # Iniciar hilo de limpieza UDP
    cleanup_thread = threading.Thread(
        target=cleanup_inactive_udp_relays, name="UdpCleanupThread", daemon=True
    )
    cleanup_thread.start()
    logging.info("Hilo de limpieza UDP iniciado.")

    logging.info(
        f"Servidor proxy iniciado. Monitoreando {len(monitored_sockets)} sockets iniciales..."
    )

    # === BUCLE PRINCIPAL ===
    try:
        while True:
            # Obtener copia segura de sockets a monitorear
            current_monitored_sockets = []
            # === Inicio Sección Crítica ===
            with relay_lock:
                current_monitored_sockets = list(monitored_sockets)
            # === Fin Sección Crítica ===

            if not current_monitored_sockets:
                logging.debug("No hay sockets para monitorear, esperando...")
                time.sleep(1.0)
                continue

            # Esperar eventos en los sockets
            try:
                readable, _, exceptional = select.select(
                    current_monitored_sockets, [], current_monitored_sockets, 1.0
                )
            except (
                ValueError
            ):  # Ocurre si un socket en la lista fue cerrado incorrectamente
                logging.warning(
                    "ValueError en select (socket inválido?). Reconstruyendo lista monitoreada..."
                )
                time.sleep(0.1)
                # Reconstruir forzosamente desde las fuentes de verdad
                with relay_lock:
                    monitored_sockets = [s for s in server_sockets] + list(
                        active_udp_relays.keys()
                    )
                continue
            except Exception as e:
                logging.error(f"Error inesperado en select: {e}")
                time.sleep(1.0)
                continue

            # --- PROCESAR SOCKETS LISTOS ---

            # Sockets listos para leer
            for sock in readable:
                socket_processed = False  # Flag para saber si ya lo manejamos

                # --- 1. Conexión TCP Entrante ---
                if sock.type == socket.SOCK_STREAM and sock in server_sockets:
                    try:
                        client_socket, client_address = sock.accept()
                        listen_port = sock.getsockname()[1]
                        # Asignar nombre al hilo para mejor logging
                        thread_name = (
                            f"TCP-Handler-{client_address[0]}-{client_address[1]}"
                        )
                        handler_thread = threading.Thread(
                            target=handle_tcp_client,
                            args=(client_socket, client_address, listen_port),
                            name=thread_name,
                            daemon=True,
                        )
                        handler_thread.start()
                        socket_processed = True
                    except Exception as e:
                        logging.error(f"Error aceptando conexión TCP: {e}")

                # --- 2. Paquete UDP Entrante (Desde Dispositivo) ---
                elif sock.type == socket.SOCK_DGRAM and sock in server_sockets:
                    try:
                        data, client_address = sock.recvfrom(BUFFER_SIZE)
                        handle_udp_from_client(
                            sock, data, client_address
                        )  # Llamar al handler UDP
                        socket_processed = True
                    except socket.error as e:
                        logging.error(
                            f"UDP: Error socket recibiendo de cliente {sock.getsockname()}: {e}"
                        )
                    except Exception as e:
                        logging.error(
                            f"UDP: Error inesperado procesando cliente {sock.getsockname()}: {e}"
                        )

                # --- 3. Paquete UDP Entrante (Desde Traccar en Relay Socket) ---
                # Usar elif aquí es importante para no procesar dos veces si algo sale mal
                elif sock.type == socket.SOCK_DGRAM and not socket_processed:
                    # Este *debe* ser un socket relay si no es de escucha
                    is_active_relay = False
                    relay_info_str = "desconocido"  # Para logs
                    # === Inicio Sección Crítica ===
                    with relay_lock:
                        if sock in active_udp_relays:
                            is_active_relay = True
                            # Obtener info *dentro* del lock por si acaso
                            relay_info_str = f"relay {sock.getsockname() if hasattr(sock,'getsockname') else '?'}"
                    # === Fin Sección Crítica ===

                    if is_active_relay:
                        logging.debug(
                            f"UDP: Datos entrantes en {relay_info_str} (probablemente de Traccar)"
                        )
                        try:
                            # Leer los datos fuera del lock
                            response_data, source_address = sock.recvfrom(BUFFER_SIZE)
                            # Ahora procesar la respuesta (necesita lock de nuevo para actualizar/enviar)
                            # === Inicio Sección Crítica ===
                            with relay_lock:
                                # Re-verificar si sigue activo *después* de leer
                                if sock in active_udp_relays:
                                    client_address, _, listening_socket = (
                                        active_udp_relays[sock]
                                    )
                                    logging.debug(
                                        f"UDP: Respuesta IN <= {source_address} via {relay_info_str} para {client_address} ({len(response_data)} bytes)"
                                    )
                                    # Actualizar timestamp
                                    active_udp_relays[sock] = (
                                        client_address,
                                        time.time(),
                                        listening_socket,
                                    )
                                    # Obtenemos el socket de escucha asociado para enviar la respuesta
                                    target_listening_socket = listening_socket
                                else:
                                    logging.warning(
                                        f"UDP: {relay_info_str} fue limpiado mientras se leían datos de {source_address}. Descartando."
                                    )
                                    target_listening_socket = None  # No enviar
                            # === Fin Sección Crítica ===

                            # Enviar respuesta al cliente original (fuera del lock)
                            if target_listening_socket:
                                try:
                                    target_listening_socket.sendto(
                                        response_data, client_address
                                    )
                                    logging.debug(
                                        f"UDP: Respuesta OUT => Cliente {client_address} desde {target_listening_socket.getsockname()}"
                                    )
                                except socket.error as e_send:
                                    logging.error(
                                        f"UDP: Error enviando respuesta a {client_address}: {e_send}"
                                    )
                                except Exception as e_send_other:
                                    logging.error(
                                        f"UDP: Error inesperado enviando respuesta a {client_address}: {e_send_other}"
                                    )

                        except (socket.error, OSError) as e_recv:
                            # Manejar error de lectura (incluye Bad file descriptor)
                            logging.error(
                                f"UDP: Error socket/OS al recibir en {relay_info_str}: {e_recv}"
                            )
                            # Limpiar el relay problemático AHORA
                            # === Inicio Sección Crítica ===
                            with relay_lock:
                                if sock in active_udp_relays:
                                    client_addr, _, _ = active_udp_relays.pop(
                                        sock, (None, None, None)
                                    )
                                    if (
                                        client_addr
                                        and client_addr in client_to_relay_socket
                                    ):
                                        del client_to_relay_socket[client_addr]
                                    if sock in monitored_sockets:
                                        monitored_sockets.remove(sock)
                                    logging.info(
                                        f"UDP: Relay para {client_addr} eliminado debido a error de recepción."
                                    )
                                else:
                                    # Si no está en active_relays, solo asegurar que no esté en monitored
                                    if sock in monitored_sockets:
                                        monitored_sockets.remove(sock)
                            # === Fin Sección Crítica ===
                            try:
                                sock.close()  # Cerrar fuera del lock
                            except:
                                pass
                        except Exception as e_recv_other:
                            logging.error(
                                f"UDP: Error inesperado al recibir en {relay_info_str}: {e_recv_other}"
                            )
                            # Limpiar por seguridad (mismo código de limpieza anterior)
                            with relay_lock:
                                if sock in active_udp_relays:
                                    client_addr, _, _ = active_udp_relays.pop(
                                        sock, (None, None, None)
                                    )
                                    if (
                                        client_addr
                                        and client_addr in client_to_relay_socket
                                    ):
                                        del client_to_relay_socket[client_addr]
                                    if sock in monitored_sockets:
                                        monitored_sockets.remove(sock)
                                    logging.info(
                                        f"UDP: Relay para {client_addr} eliminado debido a error inesperado."
                                    )
                                else:
                                    if sock in monitored_sockets:
                                        monitored_sockets.remove(sock)
                            try:
                                sock.close()
                            except:
                                pass
                    else:
                        # Socket UDP no es de escucha y tampoco es un relay activo conocido?
                        logging.warning(
                            f"UDP: Datos recibidos en socket UDP desconocido/inactivo {sock.getsockname() if hasattr(sock,'getsockname') else '?'}. Ignorando."
                        )
                        # Intentar limpiarlo de monitored_sockets si está allí
                        with relay_lock:
                            if sock in monitored_sockets:
                                monitored_sockets.remove(sock)
                        try:
                            sock.close()
                        except:
                            pass

            # --- PROCESAR SOCKETS CON ERROR ---
            for sock in exceptional:
                sock_info = (
                    sock.getsockname()
                    if hasattr(sock, "getsockname")
                    else "desconocido"
                )
                logging.error(
                    f"EXCEP: Error excepcional detectado en socket {sock_info}. Intentando limpiar."
                )
                sock_cleaned = False
                # === Inicio Sección Crítica ===
                with relay_lock:
                    if sock in active_udp_relays:  # Es un relay UDP
                        client_addr, _, _ = active_udp_relays.pop(
                            sock, (None, None, None)
                        )
                        if client_addr and client_addr in client_to_relay_socket:
                            del client_to_relay_socket[client_addr]
                        if sock in monitored_sockets:
                            monitored_sockets.remove(sock)
                        logging.info(f"EXCEP: Limpiando relay UDP para {client_addr}.")
                        sock_cleaned = True
                    elif sock in server_sockets:  # Es un socket de escucha
                        logging.critical(
                            f"EXCEP: Error CRÍTICO en socket de escucha {sock_info}. Requiere REINICIO."
                        )
                        # Eliminarlo para que deje de dar errores, pero el puerto ya no funcionará
                        if sock in monitored_sockets:
                            monitored_sockets.remove(sock)
                        server_sockets.remove(sock)
                        sock_cleaned = True
                    else:  # Socket desconocido? Intentar eliminar de monitored por si acaso
                        if sock in monitored_sockets:
                            monitored_sockets.remove(sock)
                            logging.warning(
                                f"EXCEP: Socket desconocido eliminado de monitoreo."
                            )
                            sock_cleaned = True
                # === Fin Sección Crítica ===
                if sock_cleaned:  # Cerrar fuera del lock si lo identificamos/limpiamos
                    try:
                        sock.close()
                    except Exception as e_close:
                        logging.error(
                            f"EXCEP: Error cerrando socket {sock_info}: {e_close}"
                        )

    except KeyboardInterrupt:
        logging.info("Cierre solicitado (Ctrl+C).")
    except Exception as main_loop_e:
        logging.critical(
            f"Error fatal en bucle principal: {main_loop_e}", exc_info=True
        )  # Loggear traceback
    finally:
        logging.info("Iniciando cierre del servidor...")
        # Señalizar al sender JSON que termine (opcional, si queremos que procese lo que queda)
        # JSON_QUEUE.join() # Esperar a que la cola se vacíe (puede tardar)
        # O simplemente cerrar todo:
        logging.info("Cerrando todos los sockets...")
        with relay_lock:
            sockets_to_close = list(server_sockets) + list(active_udp_relays.keys())
        closed_count = 0
        for sock in sockets_to_close:
            try:
                sock.close()
                closed_count += 1
            except Exception as e:
                logging.debug(f"Error cerrando socket: {e}")  # Debug en cierre
        logging.info(f"Se intentó cerrar {closed_count} sockets. Servidor detenido.")
        logging.shutdown()


# --- Punto de Entrada ---
if __name__ == "__main__":
    setup_logging()
    start_server()
