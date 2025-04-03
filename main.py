import socket
import json
import threading
import select
import time
import sys
import os
import logging  # Importar el módulo de logging
from datetime import datetime

# --- Configuración ---
LISTEN_IP = "0.0.0.0"
TRACCAR_HOST = "localhost"
JSON_FORWARD_HOST = "localhost"
JSON_FORWARD_PORT = 7005
OMIT_FILENAME = "omit_list.txt"  # Nombre del archivo para omitir IDs

DEVICE_PORTS_MAP = {
    6013: 5013,  # Sinotrack (UDP/TCP)
    6001: 5001,  # Coban (TCP)
    6027: 5027,  # Teltonika (TCP)
    # Añade más mapeos si es necesario
}

# --- Gestión de Omisión ---
omit_identifiers = set()  # Se cargará desde el archivo
omit_lock = threading.Lock()

BUFFER_SIZE = 4096
SOCKET_TIMEOUT = 180  # TCP
UDP_TIMEOUT = 300  # UDP Relays
CLEANUP_INTERVAL = 60  # UDP Cleanup

# --- Estructuras de Datos para UDP con Estado ---
active_udp_relays = {}
client_to_relay_socket = {}
relay_lock = threading.Lock()
monitored_sockets = []  # Lista global de sockets para select


# --- Configuración del Logging ---
def setup_logging():
    """Configura el logging para escribir a archivo diario y a consola."""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)  # Crear directorio si no existe
    log_filename = os.path.join(
        log_dir, f"log-{datetime.now().strftime('%Y-%m-%d')}.log"
    )

    logging.basicConfig(
        level=logging.DEBUG,  # Capturar todos los niveles (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),  # Escribir a archivo
            logging.StreamHandler(sys.stdout),  # Escribir a consola también
        ],
    )
    logging.info("Logging configurado. Escribiendo a: %s", log_filename)


# --- Cargar Identificadores a Omitir ---
def load_omitted_identifiers(filename=OMIT_FILENAME):
    """Carga identificadores desde un archivo de texto."""
    loaded_ids = set()
    try:
        with open(filename, "r", encoding="utf-8") as f:
            # Lee cada línea, quita espacios en blanco, ignora líneas vacías
            loaded_ids = {line.strip() for line in f if line.strip()}

        if loaded_ids:
            with omit_lock:
                # Usar replace para asegurar que solo los del archivo estén activos
                # Si prefieres añadir: omit_identifiers.update(loaded_ids)
                omit_identifiers.clear()
                omit_identifiers.update(loaded_ids)
            logging.info(
                f"Cargados {len(loaded_ids)} identificadores para omitir desde {filename}"
            )
        else:
            logging.info(
                f"Archivo de omisión {filename} encontrado pero vacío o sin identificadores válidos."
            )

    except FileNotFoundError:
        logging.info(
            f"Archivo de omisión {filename} no encontrado. No se omitirá ningún identificador."
        )
        # Asegurarse de que el set esté vacío si el archivo no existe
        with omit_lock:
            omit_identifiers.clear()
    except Exception as e:
        logging.error(f"Error cargando archivo de omisión {filename}: {e}")
        # Mantener el set vacío en caso de error
        with omit_lock:
            omit_identifiers.clear()


# --- Función para verificar si se debe omitir ---
def should_omit(data):
    """
    Verifica si los datos deben omitirse.
    Omite SÓLO SI los datos contienen un identificador de la lista de omisión
    Y TAMBIÉN contienen la cadena específica 'tracker'.
    """
    try:
        # Intentar decodificar como texto. Si falla, no podemos verificar las cadenas.
        decoded_for_check = data.decode("utf-8", errors="ignore")

        # Variable para registrar si encontramos un ID de la lista
        identifier_found_in_message = None

        with omit_lock:  # Acceso seguro al set
            if not omit_identifiers:
                # Si no hay identificadores para omitir, no hay nada que hacer.
                return False

            # 1. Buscar si algún identificador de la lista está en el mensaje
            for identifier in omit_identifiers:
                if identifier in decoded_for_check:
                    identifier_found_in_message = (
                        identifier  # Guardamos cuál encontramos
                    )
                    break  # Encontramos uno, suficiente para este paso

        # 2. Si encontramos un identificador de la lista, verificar AHORA si contiene 'tracker'
        if identifier_found_in_message:
            # Comprobar si la cadena 'tracker' está presente (sensible a mayúsculas/minúsculas)
            # Si necesitas que sea insensible, usa: "tracker".lower() in decoded_for_check.lower()
            if "tracker" in decoded_for_check:
                # ¡Ambas condiciones se cumplen! Omitir este mensaje.
                logging.info(
                    f"Omitiendo mensaje que contiene 'tracker' del identificador omitido: {identifier_found_in_message}"
                )
                return True  # Sí, omitir
            else:
                # El identificador está, pero 'tracker' no. NO omitir este mensaje.
                logging.debug(
                    f"Mensaje del identificador omitido {identifier_found_in_message} permitido (no contiene 'tracker')."
                )
                return False  # No omitir
        else:
            # No se encontró ningún identificador de la lista en el mensaje. No omitir.
            return False

    except UnicodeDecodeError:
        # Si no se puede decodificar como UTF-8, no podemos buscar las cadenas. No omitir.
        logging.debug(
            "No se pudo decodificar el mensaje como UTF-8 para verificar omisión."
        )
        return False
    except Exception as e:
        # En caso de cualquier otro error, es más seguro no omitir.
        logging.error(f"Error inesperado durante la verificación de omisión: {e}")
        return False  # No omitir por defecto en caso de error


# --- Función para enviar datos al puerto JSON ---
def forward_to_json_port(listen_port, data, source_address, protocol="TCP"):
    # (Lógica sin cambios, solo usa logging)
    json_socket = None
    try:
        try:
            decoded_data = data.decode("utf-8", errors="replace")
        except UnicodeDecodeError:
            decoded_data = data.hex()
        payload = {
            "protocol": protocol,
            "port": listen_port,
            "source_ip": source_address[0],
            "source_port": source_address[1],
            "traccar_port": DEVICE_PORTS_MAP.get(listen_port),
            "data": decoded_data,
            "timestamp": time.time(),
        }
        json_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        json_socket.settimeout(1)
        json_socket.connect((JSON_FORWARD_HOST, JSON_FORWARD_PORT))
        json_socket.sendall(json.dumps(payload).encode("utf-8"))
    except socket.timeout:
        logging.error(
            f"Timeout al conectar/enviar a JSON Port {JSON_FORWARD_HOST}:{JSON_FORWARD_PORT}"
        )
    except socket.error as e:
        logging.debug(
            f"No se pudo enviar a JSON Port {JSON_FORWARD_HOST}:{JSON_FORWARD_PORT}: {e}"
        )
    except Exception as e:
        logging.error(f"Error inesperado al enviar a JSON Port: {e}")
    finally:
        if json_socket:
            try:
                json_socket.close()
            except socket.error:
                pass


# --- Manejador para Conexiones TCP ---
def handle_tcp_client(client_socket, client_address, listen_port):
    # (Lógica sin cambios, solo usa logging y llama a should_omit)
    traccar_port = DEVICE_PORTS_MAP.get(listen_port)
    if not traccar_port:
        logging.error(f"TCP: No se encontró mapeo de puerto Traccar para {listen_port}")
        client_socket.close()
        return

    logging.info(
        f"TCP: Conexión de {client_address} en puerto {listen_port} -> Traccar {TRACCAR_HOST}:{traccar_port}"
    )
    traccar_socket = None
    connection_active_time = time.time()
    try:
        traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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
                        f"TCP: Timeout ({SOCKET_TIMEOUT}s) de inactividad para {client_address}. Cerrando."
                    )
                    running = False
                    break
                for sock in exceptional:
                    logging.error(
                        f"TCP: Error excepcional en socket ({'cliente' if sock == client_socket else 'Traccar'}) para {client_address}. Cerrando."
                    )
                    running = False
                    if sock in sockets:
                        sockets.remove(sock)
                    if sock in readable:
                        readable.remove(sock)
                    try:
                        sock.close()
                    except:
                        pass
                    other_sock = (
                        traccar_socket if sock == client_socket else client_socket
                    )
                    if other_sock in sockets:
                        sockets.remove(other_sock)
                    try:
                        other_sock.close()
                    except:
                        pass
                    break
                if not running:
                    break
                for sock in readable:
                    data = sock.recv(BUFFER_SIZE)
                    if not data:
                        logging.info(
                            f"TCP: Conexión cerrada por {'cliente' if sock == client_socket else 'Traccar'} para {client_address}. Duración: {time.time() - connection_active_time:.2f}s. Cerrando ambos."
                        )
                        running = False
                        break
                    if sock == client_socket:
                        source_name, dest_name = (
                            f"Cliente {client_address}",
                            f"Traccar {TRACCAR_HOST}:{traccar_port}",
                        )
                        logging.debug(
                            f"TCP: {source_name} -> {dest_name} ({len(data)} bytes)"
                        )
                        if should_omit(data):  # <-- Llama a la función de omisión
                            logging.debug(f"TCP: Datos de {source_name} OMITIDOS.")
                            continue
                        traccar_socket.sendall(data)
                        forward_to_json_port(listen_port, data, client_address, "TCP")
                    else:
                        source_name, dest_name = (
                            f"Traccar {TRACCAR_HOST}:{traccar_port}",
                            f"Cliente {client_address}",
                        )
                        logging.debug(
                            f"TCP: {source_name} -> {dest_name} ({len(data)} bytes)"
                        )
                        client_socket.sendall(data)
            except socket.timeout:
                logging.info(
                    f"TCP: Socket timeout inesperado para {client_address}. Cerrando."
                )
                running = False
            except socket.error as e:
                logging.error(
                    f"TCP: Error de socket durante recv/send para {client_address}: {e}. Cerrando."
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
            f"TCP: Timeout conectando a Traccar {TRACCAR_HOST}:{traccar_port} para cliente {client_address}"
        )
    except socket.error as e:
        logging.error(
            f"TCP: Error de socket (conexión/inicial) {client_address} -> Traccar {TRACCAR_HOST}:{traccar_port}: {e}"
        )
    except Exception as e:
        logging.error(
            f"TCP: Error general configurando conexión para {client_address}: {e}"
        )
    finally:
        logging.debug(f"TCP: Limpiando conexión para {client_address}")
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
            f"TCP: Conexión finalizada para {client_address} en puerto {listen_port}. Duración total: {time.time() - connection_active_time:.2f}s"
        )


# --- NUEVA FUNCIÓN: Manejador para Paquetes UDP (Cliente -> Proxy) ---
def handle_udp_from_client(listening_socket, data, client_address):
    """Procesa un paquete UDP recibido de un dispositivo."""
    # Accede a globales necesarios para la gestión de relays
    global monitored_sockets, active_udp_relays, client_to_relay_socket, relay_lock
    listen_port = listening_socket.getsockname()[1]
    traccar_port = DEVICE_PORTS_MAP.get(listen_port)
    if not traccar_port:
        logging.warning(
            f"UDP: Puerto {listen_port} no encontrado en DEVICE_PORTS_MAP. Paquete de {client_address} ignorado."
        )
        return

    logging.debug(
        f"UDP: Datos IN <= Cliente {client_address} en puerto {listen_port} ({len(data)} bytes)"
    )

    if should_omit(data):  # <-- Llama a la función de omisión
        logging.debug(f"UDP: Datos de {client_address} OMITIDOS.")
        return

    forward_to_json_port(
        listen_port, data, client_address, "UDP"
    )  # Enviar copia a JSON

    relay_socket_to_use = None
    with relay_lock:  # Proteger acceso a estructuras de relays
        now = time.time()
        if client_address in client_to_relay_socket:
            existing_relay_socket = client_to_relay_socket[client_address]
            if existing_relay_socket in active_udp_relays:
                _, _, listening_sock_ref = active_udp_relays[existing_relay_socket]
                active_udp_relays[existing_relay_socket] = (
                    client_address,
                    now,
                    listening_sock_ref,
                )
                relay_socket_to_use = existing_relay_socket
                logging.debug(
                    f"UDP: Usando relay existente {existing_relay_socket.getsockname()} para {client_address}"
                )
            else:
                del client_to_relay_socket[
                    client_address
                ]  # Limpiar mapeo si relay no existe

        if relay_socket_to_use is None:
            logging.debug(f"UDP: Creando NUEVO relay para {client_address}")
            try:
                relay_socket_to_use = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                relay_socket_to_use.setblocking(False)
                relay_socket_to_use.bind((LISTEN_IP, 0))
                monitored_sockets.append(
                    relay_socket_to_use
                )  # Añadir a select globalmente
                active_udp_relays[relay_socket_to_use] = (
                    client_address,
                    now,
                    listening_socket,
                )
                client_to_relay_socket[client_address] = relay_socket_to_use
                logging.info(
                    f"UDP: Nuevo relay {relay_socket_to_use.getsockname()} creado para {client_address}"
                )
            except Exception as e:
                logging.error(
                    f"UDP: Fallo al crear socket relay para {client_address}: {e}"
                )
                if relay_socket_to_use:
                    relay_socket_to_use.close()
                relay_socket_to_use = None

    if relay_socket_to_use:  # Si se encontró o creó exitosamente
        try:
            relay_socket_to_use.sendto(data, (TRACCAR_HOST, traccar_port))
            logging.debug(
                f"UDP: Datos OUT => Traccar {TRACCAR_HOST}:{traccar_port} via {relay_socket_to_use.getsockname()} para {client_address}"
            )
        except socket.error as e:
            logging.error(
                f"UDP: Error enviando a Traccar para {client_address} via {relay_socket_to_use.getsockname()}: {e}"
            )


# --- Limpieza Periódica de Relays UDP Inactivos ---
def cleanup_inactive_udp_relays():
    # (Lógica sin cambios, solo usa logging)
    global monitored_sockets
    while True:
        time.sleep(CLEANUP_INTERVAL)
        now = time.time()
        cleaned_count = 0
        sockets_to_remove_from_monitored = []
        with relay_lock:
            sockets_to_check = list(active_udp_relays.keys())
            for relay_socket in sockets_to_check:
                if relay_socket in active_udp_relays:
                    client_address, last_activity, _ = active_udp_relays[relay_socket]
                    if now - last_activity > UDP_TIMEOUT:
                        relay_info = f"relay {relay_socket.getsockname() if hasattr(relay_socket,'getsockname') else '?'} de {client_address}"
                        logging.info(f"UDP: Timeout para {relay_info}. Limpiando.")
                        try:
                            del active_udp_relays[relay_socket]
                            if client_address in client_to_relay_socket:
                                del client_to_relay_socket[client_address]
                            sockets_to_remove_from_monitored.append(relay_socket)
                            relay_socket.close()
                            cleaned_count += 1
                        except Exception as e:
                            logging.error(
                                f"UDP: Error durante limpieza de {relay_info}: {e}"
                            )

            if sockets_to_remove_from_monitored:
                logging.debug(
                    f"UDP Cleanup: Removiendo {len(sockets_to_remove_from_monitored)} sockets de la lista monitoreada."
                )
                for sock_to_remove in sockets_to_remove_from_monitored:
                    if sock_to_remove in monitored_sockets:
                        monitored_sockets.remove(sock_to_remove)

        if cleaned_count > 0:
            logging.info(
                f"UDP: Limpieza completada. {cleaned_count} relays inactivos eliminados."
            )
        logging.debug(
            f"UDP: Relays activos: {len(active_udp_relays)}. Sockets Monitoreados: {len(monitored_sockets)}"
        )


# --- Servidor Principal ---
def start_server():
    global monitored_sockets
    server_sockets = []
    udp_listening_sockets = {}

    load_omitted_identifiers()  # Cargar IDs a omitir al inicio

    # Crear sockets de escucha (igual que antes, usa logging)
    for listen_port, traccar_port in DEVICE_PORTS_MAP.items():
        try:  # TCP
            tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            tcp_sock.bind((LISTEN_IP, listen_port))
            tcp_sock.listen(128)
            tcp_sock.setblocking(False)
            server_sockets.append(tcp_sock)
            with relay_lock:
                monitored_sockets.append(tcp_sock)  # Añadir bajo lock por seguridad
            logging.info(
                f"Escuchando TCP en {LISTEN_IP}:{listen_port} -> Traccar {TRACCAR_HOST}:{traccar_port}"
            )
        except OSError as e:
            logging.critical(f"Fallo escucha TCP en {listen_port}: {e}. Saliendo.")
            exit(1)
        try:  # UDP
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            udp_sock.bind((LISTEN_IP, listen_port))
            udp_sock.setblocking(False)
            server_sockets.append(udp_sock)
            with relay_lock:
                monitored_sockets.append(udp_sock)  # Añadir bajo lock por seguridad
            udp_listening_sockets[listen_port] = udp_sock
            logging.info(
                f"Escuchando UDP en {LISTEN_IP}:{listen_port} -> Traccar {TRACCAR_HOST}:{traccar_port}"
            )
        except OSError as e:
            logging.critical(f"Fallo escucha UDP en {listen_port}: {e}. Saliendo.")
            exit(1)

    cleanup_thread = threading.Thread(target=cleanup_inactive_udp_relays, daemon=True)
    cleanup_thread.start()
    logging.info("Hilo de limpieza UDP iniciado.")
    logging.info(f"Servidor proxy iniciado. Monitoreando sockets...")

    try:
        while True:
            current_monitored_sockets = []
            with relay_lock:  # Copiar lista monitoreada de forma segura
                current_monitored_sockets = list(monitored_sockets)

            if not current_monitored_sockets:
                logging.debug("No hay sockets para monitorear, esperando...")
                time.sleep(1.0)
                continue

            try:
                readable, _, exceptional = select.select(
                    current_monitored_sockets, [], current_monitored_sockets, 1.0
                )
            except ValueError as e:
                logging.error(
                    f"Error en select (socket inválido?): {e}. Reconstruyendo lista monitoreada..."
                )
                time.sleep(0.5)
                with relay_lock:
                    monitored_sockets = [s for s in server_sockets] + list(
                        active_udp_relays.keys()
                    )
                continue
            except Exception as e:
                logging.error(f"Error inesperado en select: {e}")
                time.sleep(1.0)
                continue

            # Procesar sockets listos para leer
            for sock in readable:
                # --- Conexión TCP Entrante ---
                if sock.type == socket.SOCK_STREAM and sock in server_sockets:
                    try:
                        client_socket, client_address = sock.accept()
                        listen_port = sock.getsockname()[1]
                        handler_thread = threading.Thread(
                            target=handle_tcp_client,
                            args=(client_socket, client_address, listen_port),
                            daemon=True,
                        )
                        handler_thread.start()
                    except Exception as e:
                        logging.error(f"Error aceptando conexión TCP: {e}")

                # --- Paquete UDP Entrante (Desde Dispositivo) ---
                elif sock.type == socket.SOCK_DGRAM and sock in server_sockets:
                    try:
                        data, client_address = sock.recvfrom(BUFFER_SIZE)
                        # Llamar a la función refactorizada
                        handle_udp_from_client(sock, data, client_address)
                    except socket.error as e:
                        logging.error(
                            f"UDP: Error de socket recibiendo de cliente en {sock.getsockname()}: {e}"
                        )
                    except Exception as e:
                        logging.error(
                            f"UDP: Error inesperado procesando paquete de cliente en {sock.getsockname()}: {e}"
                        )

                # --- Paquete UDP Entrante (Desde Traccar en Relay Socket) ---
                elif sock.type == socket.SOCK_DGRAM and sock not in server_sockets:
                    # Esta lógica maneja la respuesta de Traccar al dispositivo
                    try:
                        response_data, traccar_address = sock.recvfrom(BUFFER_SIZE)
                        with relay_lock:  # Proteger acceso a relays
                            if sock in active_udp_relays:
                                client_address, _, listening_socket = active_udp_relays[
                                    sock
                                ]
                                logging.debug(
                                    f"UDP: Respuesta IN <= Traccar {traccar_address} via {sock.getsockname()} para {client_address} ({len(response_data)} bytes)"
                                )
                                # Actualizar timestamp
                                active_udp_relays[sock] = (
                                    client_address,
                                    time.time(),
                                    listening_socket,
                                )
                                # Enviar respuesta al cliente original
                                try:
                                    listening_socket.sendto(
                                        response_data, client_address
                                    )
                                    logging.debug(
                                        f"UDP: Respuesta OUT => Cliente {client_address} desde puerto {listening_socket.getsockname()[1]}"
                                    )
                                except socket.error as e:
                                    logging.error(
                                        f"UDP: Error enviando respuesta a {client_address}: {e}"
                                    )
                            else:
                                logging.debug(
                                    f"UDP: Recibida respuesta de {traccar_address} en socket relay desconocido/limpiado {sock.getsockname()}. Descartando."
                                )
                                if sock in monitored_sockets:
                                    monitored_sockets.remove(sock)  # Limpiar de select
                                try:
                                    sock.close()
                                except:
                                    pass
                    except socket.error as e:
                        logging.error(
                            f"UDP: Error de socket recibiendo respuesta de Traccar en {sock.getsockname() if hasattr(sock,'getsockname') else 'socket desconocido'}: {e}"
                        )
                        # Limpiar relay si hay error
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
                                logging.info(
                                    f"UDP: Eliminando relay para {client_addr} debido a error de socket."
                                )
                            if sock in monitored_sockets:
                                monitored_sockets.remove(sock)
                            try:
                                sock.close()
                            except:
                                pass
                    except Exception as e:
                        logging.error(
                            f"UDP: Error inesperado procesando respuesta de Traccar: {e}"
                        )

            # --- Manejo de Sockets con Error Excepcional ---
            for sock in exceptional:
                # (Misma lógica de limpieza excepcional, usa logging)
                sock_info = (
                    sock.getsockname()
                    if hasattr(sock, "getsockname")
                    else "socket desconocido"
                )
                logging.error(
                    f"EXCEP: Error excepcional detectado en socket {sock_info}. Intentando limpiar."
                )
                sock_cleaned = False
                with relay_lock:  # Proteger acceso a estructuras globales
                    if sock in active_udp_relays:
                        client_addr, _, _ = active_udp_relays.pop(
                            sock, (None, None, None)
                        )
                        if client_addr and client_addr in client_to_relay_socket:
                            del client_to_relay_socket[client_addr]
                        logging.info(
                            f"EXCEP: Limpiando relay UDP para {client_addr} debido a error."
                        )
                        sock_cleaned = True
                    elif sock in server_sockets:
                        logging.critical(
                            f"EXCEP: Error crítico en socket de escucha {sock_info}. ¡Reinicio manual recomendado!"
                        )
                        sock_cleaned = True  # Marcamos como "manejado" (reportado)
                    # Eliminar de la lista monitoreada global
                    if sock in monitored_sockets:
                        monitored_sockets.remove(sock)

                if sock_cleaned:  # Solo cerrar si identificamos qué era o es crítico
                    try:
                        sock.close()
                    except Exception as e:
                        logging.error(
                            f"EXCEP: Error cerrando socket excepcional {sock_info}: {e}"
                        )

    except KeyboardInterrupt:
        logging.info("Cierre solicitado por el usuario (Ctrl+C).")
    finally:
        logging.info("Cerrando todos los sockets...")
        # (Misma lógica de cierre final, usa logging)
        with relay_lock:
            sockets_to_close = list(server_sockets) + list(active_udp_relays.keys())
        closed_count = 0
        for sock in sockets_to_close:
            try:
                sock.close()
                closed_count += 1
            except Exception as e:
                logging.error(
                    f"Error cerrando socket {sock.getsockname() if hasattr(sock, 'getsockname') else sock}: {e}"
                )
        logging.info(f"Se intentó cerrar {closed_count} sockets. Servidor detenido.")
        logging.shutdown()  # Asegurar que los handlers de logging se cierren bien


# --- Punto de Entrada ---
if __name__ == "__main__":
    setup_logging()  # Configurar el logging primero
    start_server()
