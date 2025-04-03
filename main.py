import socket
import json
import threading
import select
import time
import sys
from datetime import datetime
from collections import (
    defaultdict,
)  # Podría ser útil, pero usemos dicts normales por ahora

# --- Configuración ---
LISTEN_IP = "0.0.0.0"
TRACCAR_HOST = "localhost"
JSON_FORWARD_HOST = "localhost"
JSON_FORWARD_PORT = 7005

DEVICE_PORTS_MAP = {
    6013: 5013,  # Sinotrack (UDP/TCP)
    6001: 5001,  # Coban (TCP)
    6027: 5027,  # Teltonika (TCP)
    # Añade más mapeos si es necesario (asegúrate que los puertos aquí coinciden con los de escucha)
}

omit_identifiers = set()
omit_lock = threading.Lock()

BUFFER_SIZE = 4096
SOCKET_TIMEOUT = 180  # Timeout para inactividad TCP
UDP_TIMEOUT = 300  # Timeout para limpiar relays UDP inactivos (segundos, ej. 5 minutos)
CLEANUP_INTERVAL = 60  # Intervalo para ejecutar la limpieza de UDP (segundos)

# --- Estructuras de Datos para UDP con Estado ---
# Mapeo: socket_intermediario (el que habla con Traccar) -> (client_address, last_activity, listening_socket)
active_udp_relays = {}
# Mapeo inverso para búsqueda rápida: client_address -> socket_intermediario
client_to_relay_socket = {}
# Bloqueo para proteger el acceso concurrente a las estructuras de relays UDP
relay_lock = threading.Lock()
# Lista de sockets activos para select (incluirá sockets de escucha y sockets intermediarios UDP)
monitored_sockets = []


# --- Funciones de Logging ---
# (Iguales que antes: log_info, log_error, log_debug)
def log_info(message):
    print(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [INFO] {message}",
        file=sys.stdout,
        flush=True,
    )


def log_error(message):
    print(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [ERROR] {message}",
        file=sys.stderr,
        flush=True,
    )


def log_debug(message):
    # Descomenta la siguiente línea para debugging detallado
    print(
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [DEBUG] {message}",
        file=sys.stdout,
        flush=True,
    )
    # pass


# --- Función para enviar datos al puerto JSON ---
# (Igual que antes)
def forward_to_json_port(listen_port, data, source_address, protocol="TCP"):
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
        # log_debug(f"-> JSON OK desde {protocol} {source_address} (Puerto {listen_port})")

    except socket.timeout:
        log_error(
            f"Timeout al conectar/enviar a JSON Port {JSON_FORWARD_HOST}:{JSON_FORWARD_PORT}"
        )
    except socket.error as e:
        log_debug(
            f"No se pudo enviar a JSON Port {JSON_FORWARD_HOST}:{JSON_FORWARD_PORT}: {e}"
        )
    except Exception as e:
        log_error(f"Error inesperado al enviar a JSON Port: {e}")
    finally:
        if json_socket:
            try:
                json_socket.close()
            except socket.error:
                pass


# --- Función para verificar si se debe omitir ---
# (Igual que antes)
def should_omit(data):
    try:
        decoded_for_check = data.decode("utf-8", errors="ignore")
        with omit_lock:
            for identifier in omit_identifiers:
                if identifier in decoded_for_check:
                    log_info(f"Omitiendo datos para identificador: {identifier}")
                    return True
    except Exception:
        pass
    return False


# --- Manejador para Conexiones TCP ---
# (Misma función robusta de la versión anterior, sin cambios necesarios aquí)
def handle_tcp_client(client_socket, client_address, listen_port):
    traccar_port = DEVICE_PORTS_MAP.get(listen_port)
    if not traccar_port:
        log_error(f"TCP: No se encontró mapeo de puerto Traccar para {listen_port}")
        client_socket.close()
        return

    log_info(
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
        log_debug(f"TCP: Conexión establecida con Traccar para {client_address}")

        sockets = [client_socket, traccar_socket]
        running = True
        while running:
            try:
                readable, _, exceptional = select.select(
                    sockets, [], sockets, SOCKET_TIMEOUT
                )

                if not readable and not exceptional:
                    log_info(
                        f"TCP: Timeout ({SOCKET_TIMEOUT}s) de inactividad para {client_address}. Cerrando."
                    )
                    running = False
                    break

                for sock in exceptional:
                    log_error(
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
                        log_info(
                            f"TCP: Conexión cerrada por {'cliente' if sock == client_socket else 'Traccar'} para {client_address}. Duración: {time.time() - connection_active_time:.2f}s. Cerrando ambos."
                        )
                        running = False
                        break

                    if sock == client_socket:
                        source_name = f"Cliente {client_address}"
                        destination_socket = traccar_socket
                        dest_name = f"Traccar {TRACCAR_HOST}:{traccar_port}"
                        log_debug(
                            f"TCP: {source_name} -> {dest_name} ({len(data)} bytes)"
                        )
                        if should_omit(data):
                            log_debug(f"TCP: Datos de {source_name} OMITIDOS.")
                            continue
                        destination_socket.sendall(data)  # Enviar a Traccar primero
                        forward_to_json_port(
                            listen_port, data, client_address, "TCP"
                        )  # Luego a JSON
                    else:  # sock == traccar_socket
                        source_name = f"Traccar {TRACCAR_HOST}:{traccar_port}"
                        destination_socket = client_socket
                        dest_name = f"Cliente {client_address}"
                        log_debug(
                            f"TCP: {source_name} -> {dest_name} ({len(data)} bytes)"
                        )
                        destination_socket.sendall(data)  # Solo reenviar a cliente

            except socket.timeout:
                log_info(
                    f"TCP: Socket timeout inesperado para {client_address}. Cerrando."
                )
                running = False
            except socket.error as e:
                log_error(
                    f"TCP: Error de socket durante recv/send para {client_address}: {e}. Cerrando."
                )
                running = False
            except Exception as e:
                log_error(
                    f"TCP: Error inesperado manejando {client_address}: {e}. Cerrando."
                )
                running = False

            if not running:
                break

    except socket.timeout:
        log_error(
            f"TCP: Timeout conectando a Traccar {TRACCAR_HOST}:{traccar_port} para cliente {client_address}"
        )
    except socket.error as e:
        log_error(
            f"TCP: Error de socket (conexión/inicial) {client_address} -> Traccar {TRACCAR_HOST}:{traccar_port}: {e}"
        )
    except Exception as e:
        log_error(
            f"TCP: Error general configurando conexión para {client_address}: {e}"
        )
    finally:
        log_debug(f"TCP: Limpiando conexión para {client_address}")
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
        log_info(
            f"TCP: Conexión finalizada para {client_address} en puerto {listen_port}. Duración total: {time.time() - connection_active_time:.2f}s"
        )


# --- Limpieza Periódica de Relays UDP Inactivos ---
def cleanup_inactive_udp_relays():
    while True:
        time.sleep(CLEANUP_INTERVAL)
        now = time.time()
        cleaned_count = 0
        with relay_lock:
            # Es necesario copiar las keys porque vamos a modificar el diccionario mientras iteramos
            sockets_to_check = list(active_udp_relays.keys())
            for relay_socket in sockets_to_check:
                if (
                    relay_socket in active_udp_relays
                ):  # Verificar si aún existe (podría haberse eliminado)
                    client_address, last_activity, _ = active_udp_relays[relay_socket]
                    if now - last_activity > UDP_TIMEOUT:
                        log_info(
                            f"UDP: Timeout para relay de {client_address}. Limpiando."
                        )
                        try:
                            # Eliminar de estructuras de datos
                            del active_udp_relays[relay_socket]
                            if client_address in client_to_relay_socket:
                                del client_to_relay_socket[client_address]
                            # Eliminar de la lista de sockets monitoreados
                            if relay_socket in monitored_sockets:
                                monitored_sockets.remove(relay_socket)
                            # Cerrar el socket intermediario
                            relay_socket.close()
                            cleaned_count += 1
                        except Exception as e:
                            log_error(
                                f"UDP: Error durante limpieza de {client_address}: {e}"
                            )
        if cleaned_count > 0:
            log_info(
                f"UDP: Limpieza completada. {cleaned_count} relays inactivos eliminados."
            )
        log_debug(f"UDP: Relays activos: {len(active_udp_relays)}")


# --- Servidor Principal ---
def start_server():
    global monitored_sockets  # Para poder modificarla globalmente
    server_sockets = []  # Sockets de escucha (TCP y UDP)
    udp_listening_sockets = {}  # Mapeo: listen_port -> udp_listening_socket

    # Crear sockets de escucha TCP y UDP
    for listen_port, traccar_port in DEVICE_PORTS_MAP.items():
        # Crear socket TCP
        try:
            tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            tcp_sock.bind((LISTEN_IP, listen_port))
            tcp_sock.listen(128)
            tcp_sock.setblocking(False)
            server_sockets.append(tcp_sock)
            monitored_sockets.append(tcp_sock)  # Añadir a la lista global para select
            log_info(
                f"Escuchando TCP en {LISTEN_IP}:{listen_port} -> Traccar {TRACCAR_HOST}:{traccar_port}"
            )
        except OSError as e:
            log_error(
                f"CRITICO: Error al iniciar escucha TCP en puerto {listen_port}: {e}. Saliendo."
            )
            # Cerrar sockets ya abiertos
            for s in server_sockets:
                s.close()
            return

        # Crear socket UDP
        try:
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            udp_sock.bind((LISTEN_IP, listen_port))
            udp_sock.setblocking(False)
            server_sockets.append(udp_sock)
            monitored_sockets.append(udp_sock)  # Añadir a la lista global para select
            udp_listening_sockets[listen_port] = udp_sock  # Guardar referencia
            log_info(
                f"Escuchando UDP en {LISTEN_IP}:{listen_port} -> Traccar {TRACCAR_HOST}:{traccar_port}"
            )
        except OSError as e:
            log_error(
                f"CRITICO: Error al iniciar escucha UDP en puerto {listen_port}: {e}. Saliendo."
            )
            # Cerrar sockets ya abiertos
            for s in server_sockets:
                s.close()
            return

    # Iniciar hilo de limpieza para relays UDP
    cleanup_thread = threading.Thread(target=cleanup_inactive_udp_relays, daemon=True)
    cleanup_thread.start()
    log_info("Hilo de limpieza UDP iniciado.")

    log_info(
        f"Servidor proxy iniciado. Monitoreando {len(monitored_sockets)} sockets iniciales..."
    )

    try:
        while True:
            # Copiamos la lista de sockets a monitorear en cada iteración
            # por si cambia debido a la creación/eliminación de relays UDP
            current_monitored_sockets = []
            with relay_lock:
                current_monitored_sockets = list(
                    monitored_sockets
                )  # Crear copia segura

            if not current_monitored_sockets:
                log_info("No hay sockets para monitorear, esperando...")
                time.sleep(1.0)
                continue

            # Usar select para manejar múltiples sockets
            try:
                readable, _, exceptional = select.select(
                    current_monitored_sockets, [], current_monitored_sockets, 1.0
                )
            except ValueError as e:
                log_error(
                    f"Error en select (posiblemente socket inválido): {e}. Verificando sockets..."
                )
                # Podríamos intentar identificar y eliminar sockets malos aquí, pero es complejo
                time.sleep(1.0)  # Esperar antes de reintentar
                continue
            except Exception as e:
                log_error(f"Error inesperado en select: {e}")
                time.sleep(1.0)
                continue

            for sock in readable:
                # --- Manejo Conexión TCP Entrante ---
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
                        log_error(f"Error aceptando conexión TCP: {e}")

                # --- Manejo Paquete UDP Entrante (Desde Dispositivo) ---
                elif sock.type == socket.SOCK_DGRAM and sock in server_sockets:
                    try:
                        data, client_address = sock.recvfrom(BUFFER_SIZE)
                        listen_port = sock.getsockname()[1]
                        traccar_port = DEVICE_PORTS_MAP.get(listen_port)
                        if not traccar_port:
                            continue  # Puerto no mapeado

                        log_debug(
                            f"UDP: Datos IN <= Cliente {client_address} en puerto {listen_port} ({len(data)} bytes)"
                        )

                        if should_omit(data):
                            log_debug(f"UDP: Datos de {client_address} OMITIDOS.")
                            continue

                        # Enviar copia a JSON ahora
                        forward_to_json_port(listen_port, data, client_address, "UDP")

                        relay_socket_to_use = None
                        with relay_lock:
                            now = time.time()
                            # Buscar relay existente
                            if client_address in client_to_relay_socket:
                                existing_relay_socket = client_to_relay_socket[
                                    client_address
                                ]
                                # Verificar si el socket aún está activo (podría haberse limpiado)
                                if existing_relay_socket in active_udp_relays:
                                    # Actualizar timestamp y usarlo
                                    _, _, listening_sock_ref = active_udp_relays[
                                        existing_relay_socket
                                    ]
                                    active_udp_relays[existing_relay_socket] = (
                                        client_address,
                                        now,
                                        listening_sock_ref,
                                    )
                                    relay_socket_to_use = existing_relay_socket
                                    log_debug(
                                        f"UDP: Usando relay existente para {client_address}"
                                    )
                                else:
                                    # El mapeo inverso existía pero el relay no, limpiar mapeo inverso
                                    del client_to_relay_socket[client_address]

                            # Si no se encontró o el existente se limpió, crear uno nuevo
                            if relay_socket_to_use is None:
                                log_debug(
                                    f"UDP: Creando NUEVO relay para {client_address}"
                                )
                                try:
                                    relay_socket_to_use = socket.socket(
                                        socket.AF_INET, socket.SOCK_DGRAM
                                    )
                                    relay_socket_to_use.setblocking(False)
                                    relay_socket_to_use.bind(
                                        (LISTEN_IP, 0)
                                    )  # Puerto efímero
                                    monitored_sockets.append(
                                        relay_socket_to_use
                                    )  # Añadir a select
                                    active_udp_relays[relay_socket_to_use] = (
                                        client_address,
                                        now,
                                        sock,
                                    )  # Guardar mapeo principal
                                    client_to_relay_socket[client_address] = (
                                        relay_socket_to_use  # Guardar mapeo inverso
                                    )
                                except Exception as e:
                                    log_error(
                                        f"UDP: Fallo al crear socket relay para {client_address}: {e}"
                                    )
                                    if relay_socket_to_use:
                                        relay_socket_to_use.close()  # Cerrar si se creó parcialmente
                                    relay_socket_to_use = None  # No continuar si falló

                        # Enviar datos a Traccar usando el socket relay (si se creó/encontró)
                        if relay_socket_to_use:
                            try:
                                relay_socket_to_use.sendto(
                                    data, (TRACCAR_HOST, traccar_port)
                                )
                                log_debug(
                                    f"UDP: Datos OUT => Traccar {TRACCAR_HOST}:{traccar_port} via {relay_socket_to_use.getsockname()} para {client_address}"
                                )
                            except socket.error as e:
                                log_error(
                                    f"UDP: Error enviando a Traccar para {client_address}: {e}"
                                )
                                # Considerar limpiar este relay si falla el envío? Podría ser temporal.

                    except socket.error as e:
                        log_error(f"UDP: Error de socket recibiendo de cliente: {e}")
                    except Exception as e:
                        log_error(
                            f"UDP: Error inesperado procesando paquete de cliente: {e}"
                        )

                # --- Manejo Paquete UDP Entrante (Desde Traccar, en un socket intermediario) ---
                elif sock.type == socket.SOCK_DGRAM and sock not in server_sockets:
                    try:
                        response_data, traccar_address = sock.recvfrom(BUFFER_SIZE)
                        # Verificar que viene de Traccar (opcional pero bueno)
                        # if traccar_address[0] != TRACCAR_HOST: continue

                        with relay_lock:
                            # Buscar a qué cliente original pertenece esta respuesta
                            if sock in active_udp_relays:
                                client_address, _, listening_socket = active_udp_relays[
                                    sock
                                ]
                                log_debug(
                                    f"UDP: Respuesta IN <= Traccar {traccar_address} para {client_address} ({len(response_data)} bytes)"
                                )

                                # Actualizar timestamp de actividad para este relay
                                active_udp_relays[sock] = (
                                    client_address,
                                    time.time(),
                                    listening_socket,
                                )

                                # Enviar respuesta al cliente original usando el socket de escucha original
                                try:
                                    listening_socket.sendto(
                                        response_data, client_address
                                    )
                                    log_debug(
                                        f"UDP: Respuesta OUT => Cliente {client_address} desde puerto {listening_socket.getsockname()[1]}"
                                    )
                                except socket.error as e:
                                    log_error(
                                        f"UDP: Error enviando respuesta a {client_address}: {e}"
                                    )
                            else:
                                # Recibimos datos en un socket que ya no está en el mapeo (quizás limpiado?)
                                log_debug(
                                    f"UDP: Recibida respuesta de {traccar_address} en socket relay desconocido/limpiado {sock.getsockname()}. Descartando."
                                )
                                # Cerrar y eliminar este socket de monitored_sockets por si acaso
                                if sock in monitored_sockets:
                                    monitored_sockets.remove(sock)
                                try:
                                    sock.close()
                                except:
                                    pass

                    except socket.error as e:
                        log_error(
                            f"UDP: Error de socket recibiendo respuesta de Traccar: {e}"
                        )
                        # Si hay error en este socket, quizás deberíamos limpiarlo
                        with relay_lock:
                            if sock in active_udp_relays:
                                client_addr, _, _ = active_udp_relays[sock]
                                log_info(
                                    f"UDP: Eliminando relay para {client_addr} debido a error de socket."
                                )
                                del active_udp_relays[sock]
                                if client_addr in client_to_relay_socket:
                                    del client_to_relay_socket[client_addr]
                            if sock in monitored_sockets:
                                monitored_sockets.remove(sock)
                            try:
                                sock.close()
                            except:
                                pass
                    except Exception as e:
                        log_error(
                            f"UDP: Error inesperado procesando respuesta de Traccar: {e}"
                        )

            # --- Manejo de Sockets con Error Excepcional ---
            for sock in exceptional:
                log_error(
                    f"EXCEP: Error excepcional detectado en socket {sock.getsockname()}. Intentando limpiar."
                )
                with relay_lock:
                    # Determinar tipo de socket y limpiar adecuadamente
                    if sock in active_udp_relays:  # Es un socket relay UDP
                        client_addr, _, _ = active_udp_relays.pop(
                            sock, (None, None, None)
                        )
                        if client_addr and client_addr in client_to_relay_socket:
                            del client_to_relay_socket[client_addr]
                        log_info(
                            f"EXCEP: Limpiando relay UDP para {client_addr} debido a error."
                        )
                    elif sock in server_sockets:  # Es un socket de escucha
                        log_error(
                            f"EXCEP: Error crítico en socket de escucha {sock.getsockname()}. ¡Se requiere reinicio!"
                        )
                        # Podríamos intentar removerlo, pero es más seguro reiniciar el servicio.
                        # server_sockets.remove(sock) # Riesgoso

                    # Eliminar de la lista monitoreada y cerrar
                    if sock in monitored_sockets:
                        monitored_sockets.remove(sock)
                    try:
                        sock.close()
                    except Exception as e:
                        log_error(f"EXCEP: Error cerrando socket excepcional: {e}")

    except KeyboardInterrupt:
        log_info("Cierre solicitado por el usuario (Ctrl+C).")
    finally:
        log_info("Cerrando todos los sockets...")
        with relay_lock:
            sockets_to_close = list(server_sockets) + list(active_udp_relays.keys())
        for sock in sockets_to_close:
            try:
                sock.close()
            except Exception as e:
                log_error(
                    f"Error cerrando socket {sock.getsockname() if hasattr(sock, 'getsockname') else sock}: {e}"
                )
        log_info("Servidor detenido.")


# --- Punto de Entrada ---
if __name__ == "__main__":
    start_server()
