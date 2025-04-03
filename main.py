import socket
import json
import threading
import select
import time
import sys
from datetime import datetime

# --- Configuración ---
LISTEN_IP = "0.0.0.0"  # Escuchar en todas las interfaces
TRACCAR_HOST = "localhost"  # IP o hostname del servidor Traccar
JSON_FORWARD_HOST = "localhost"  # IP del servicio que recibe JSON
JSON_FORWARD_PORT = 7005  # Puerto del servicio que recibe JSON

# Mapeo: Puerto de escucha local -> Puerto de destino en Traccar
DEVICE_PORTS_MAP = {
    # Puerto local : Puerto Traccar
    6013: 5013,  # Sinotrack
    6001: 5001,  # Coban
    6027: 5027,  # Teltonika
    # Añade más mapeos si es necesario
}

# Lista de números de teléfono (o identificadores) a ignorar
# Usa un set para búsquedas rápidas
omit_identifiers = set()
omit_lock = threading.Lock()

BUFFER_SIZE = 4096  # Aumentar buffer puede ayudar con tramas grandes
SOCKET_TIMEOUT = 60  # Timeout para inactividad en conexiones TCP (segundos)


# --- Funciones de Logging ---
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
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [DEBUG] {message}", file=sys.stdout, flush=True)
    pass


# --- Función para enviar datos al puerto JSON ---
# (Mantenida simple, pero considera optimizar si es cuello de botella)
def forward_to_json_port(listen_port, data, source_address, protocol="TCP"):
    json_socket = None
    try:
        # Intentar decodificar como UTF-8 (común) o representar como HEX si falla
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
        json_socket.settimeout(1)  # Timeout corto para conectar/enviar
        json_socket.connect((JSON_FORWARD_HOST, JSON_FORWARD_PORT))
        json_socket.sendall(json.dumps(payload).encode("utf-8"))
        log_debug(
            f"Datos enviados a JSON Port desde {protocol} {source_address} (Puerto {listen_port})"
        )

    except socket.timeout:
        log_error(
            f"Timeout al conectar/enviar a JSON Port {JSON_FORWARD_HOST}:{JSON_FORWARD_PORT}"
        )
    except socket.error as e:
        # Falla silenciosa si el puerto JSON no está disponible o rechaza
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
                pass  # Ignorar errores al cerrar


# --- Función para verificar si se debe omitir ---
def should_omit(data):
    try:
        # Intentar decodificar como texto para buscar el identificador
        # Asume que el identificador es parte del texto (ej. número de teléfono)
        decoded_for_check = data.decode("utf-8", errors="ignore")
        with omit_lock:
            for identifier in omit_identifiers:
                if identifier in decoded_for_check:
                    log_info(f"Omitiendo datos para identificador: {identifier}")
                    return True
    except Exception:
        # Si no se puede decodificar, no podemos verificar, así que no omitimos
        pass
    return False


# --- Manejador para Conexiones TCP ---
def handle_tcp_client(client_socket, client_address, listen_port):
    traccar_port = DEVICE_PORTS_MAP.get(listen_port)
    if not traccar_port:
        log_error(f"No se encontró mapeo de puerto Traccar para {listen_port}")
        client_socket.close()
        return

    log_info(
        f"TCP Conexión aceptada de {client_address} en puerto {listen_port}, reenviando a {TRACCAR_HOST}:{traccar_port}"
    )
    traccar_socket = None

    try:
        # Conectar al servidor Traccar
        traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        traccar_socket.settimeout(10)  # Timeout para conectar a Traccar
        traccar_socket.connect((TRACCAR_HOST, traccar_port))
        traccar_socket.settimeout(SOCKET_TIMEOUT)  # Timeout de inactividad
        client_socket.settimeout(SOCKET_TIMEOUT)  # Timeout de inactividad
        log_debug(f"Conexión TCP establecida con Traccar para {client_address}")

        # Usar select para E/S no bloqueante entre cliente y Traccar
        sockets = [client_socket, traccar_socket]
        running = True
        while running:
            readable, _, exceptional = select.select(
                sockets, [], sockets, SOCKET_TIMEOUT
            )

            if not readable and not exceptional:
                # Timeout de inactividad alcanzado
                log_info(
                    f"Timeout de inactividad para TCP {client_address}. Cerrando conexión."
                )
                running = False
                break

            for sock in readable:
                try:
                    data = sock.recv(BUFFER_SIZE)
                    if not data:
                        # Conexión cerrada por el otro extremo
                        log_info(
                            f"TCP Conexión cerrada por {'cliente' if sock == client_socket else 'Traccar'} para {client_address}. Cerrando."
                        )
                        running = False
                        break

                    # Determinar origen y destino
                    if sock == client_socket:
                        source_name = f"Cliente {client_address}"
                        destination_socket = traccar_socket
                        dest_name = f"Traccar {TRACCAR_HOST}:{traccar_port}"
                        # Verificar si omitir ANTES de enviar a Traccar y JSON
                        if should_omit(data):
                            continue  # No reenviar si está omitido
                        # Enviar copia a JSON
                        forward_to_json_port(listen_port, data, client_address, "TCP")
                    else:  # sock == traccar_socket
                        source_name = f"Traccar {TRACCAR_HOST}:{traccar_port}"
                        destination_socket = client_socket
                        dest_name = f"Cliente {client_address}"
                        # Nota: No reenviamos respuestas de Traccar al JSON port,
                        #       solo el tráfico original del dispositivo. Ajustar si es necesario.

                    # Reenviar los datos
                    destination_socket.sendall(data)
                    log_debug(
                        f"{len(data)} bytes reenviados de {source_name} a {dest_name}"
                    )

                except socket.timeout:
                    log_info(
                        f"Socket timeout durante recv/send para TCP {client_address}. Cerrando."
                    )
                    running = False
                    break
                except socket.error as e:
                    log_error(
                        f"Error de socket en TCP {client_address}: {e}. Cerrando."
                    )
                    running = False
                    break
                except Exception as e:
                    log_error(
                        f"Error inesperado manejando TCP {client_address}: {e}. Cerrando."
                    )
                    running = False
                    break  # Salir del bucle for sock in readable

            for sock in exceptional:
                log_error(
                    f"Error excepcional en socket TCP para {client_address}. Cerrando."
                )
                running = False
                break  # Salir del bucle for sock in readable

            if not running:
                break  # Salir del bucle while

    except socket.timeout:
        log_error(
            f"Timeout conectando a Traccar {TRACCAR_HOST}:{traccar_port} para cliente {client_address}"
        )
    except socket.error as e:
        log_error(
            f"Error de socket conectando/manejando TCP {client_address} -> Traccar {TRACCAR_HOST}:{traccar_port}: {e}"
        )
    except Exception as e:
        log_error(f"Error general manejando TCP {client_address}: {e}")
    finally:
        # Cerrar ambos sockets de forma segura
        if client_socket:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except (socket.error, OSError):
                pass
            try:
                client_socket.close()
                log_debug(f"Socket de cliente TCP {client_address} cerrado.")
            except socket.error:
                pass
        if traccar_socket:
            try:
                traccar_socket.shutdown(socket.SHUT_RDWR)
            except (socket.error, OSError):
                pass
            try:
                traccar_socket.close()
                log_debug(f"Socket de Traccar TCP para {client_address} cerrado.")
            except socket.error:
                pass
        log_info(
            f"TCP Conexión finalizada para {client_address} en puerto {listen_port}"
        )


# --- Servidor Principal ---
def start_server():
    server_sockets = []
    udp_sockets_map = {}  # Guardar sockets UDP para reenvío

    # Crear sockets de escucha TCP y UDP
    for listen_port, traccar_port in DEVICE_PORTS_MAP.items():
        try:
            # Socket TCP
            tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            tcp_sock.bind((LISTEN_IP, listen_port))
            tcp_sock.listen(128)  # Backlog de conexiones pendientes
            tcp_sock.setblocking(False)
            server_sockets.append(tcp_sock)
            log_info(
                f"Escuchando TCP en {LISTEN_IP}:{listen_port} -> Traccar {TRACCAR_HOST}:{traccar_port}"
            )

            # Socket UDP
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            udp_sock.bind((LISTEN_IP, listen_port))
            udp_sock.setblocking(False)
            server_sockets.append(udp_sock)
            udp_sockets_map[udp_sock] = (
                listen_port,
                traccar_port,
            )  # Asociar socket con puertos
            log_info(
                f"Escuchando UDP en {LISTEN_IP}:{listen_port} -> Traccar {TRACCAR_HOST}:{traccar_port}"
            )

        except OSError as e:
            log_error(
                f"Error al iniciar escucha en puerto {listen_port}: {e}. ¿Permisos o puerto en uso?"
            )
            return  # Salir si no se puede iniciar un puerto

    log_info("Servidor proxy iniciado. Esperando conexiones...")

    try:
        while True:
            # Usar select para manejar múltiples sockets
            readable, _, exceptional = select.select(
                server_sockets, [], server_sockets, 1.0
            )  # Timeout de 1 segundo

            for sock in readable:
                if sock.type == socket.SOCK_STREAM:  # Es un socket de escucha TCP
                    try:
                        client_socket, client_address = sock.accept()
                        client_socket.setblocking(
                            True
                        )  # Poner en modo bloqueante para el hilo
                        listen_port = sock.getsockname()[1]

                        # Manejar comandos especiales (activate/desactivate) - ¡NUEVO!
                        # Intentar leer un poco para ver si es comando, con timeout corto
                        client_socket.settimeout(0.1)
                        try:
                            initial_data = client_socket.recv(100)
                            command_str = initial_data.decode(
                                "utf-8", errors="ignore"
                            ).strip()

                            handled_as_command = False
                            if command_str.startswith("desactivate:"):
                                identifier = command_str.split(":", 1)[1]
                                if identifier:
                                    with omit_lock:
                                        omit_identifiers.add(identifier)
                                    log_info(
                                        f"Identificador '{identifier}' añadido a la lista de omisión desde TCP {client_address}"
                                    )
                                    client_socket.sendall(
                                        b"OK: Desactivated\n"
                                    )  # Enviar confirmación
                                handled_as_command = True
                            elif command_str.startswith("activate:"):
                                identifier = command_str.split(":", 1)[1]
                                if identifier:
                                    with omit_lock:
                                        omit_identifiers.discard(
                                            identifier
                                        )  # discard no da error si no existe
                                    log_info(
                                        f"Identificador '{identifier}' eliminado de la lista de omisión desde TCP {client_address}"
                                    )
                                    client_socket.sendall(
                                        b"OK: Activated\n"
                                    )  # Enviar confirmación
                                handled_as_command = True

                            if handled_as_command:
                                client_socket.close()  # Cerrar conexión si era solo un comando
                                continue  # Ir al siguiente socket
                            else:
                                # No era comando, devolver el socket a modo bloqueante normal y procesar
                                client_socket.settimeout(SOCKET_TIMEOUT)
                                # ¡Necesitamos reenviar estos datos iniciales también!
                                # Iniciar hilo para manejar el resto de la conexión TCP
                                handler_thread = threading.Thread(
                                    target=handle_tcp_client_with_initial_data,
                                    args=(
                                        client_socket,
                                        client_address,
                                        listen_port,
                                        initial_data,
                                    ),
                                )
                                handler_thread.daemon = True
                                handler_thread.start()

                        except socket.timeout:
                            # No llegaron datos iniciales rápidamente, proceder normalmente
                            client_socket.settimeout(SOCKET_TIMEOUT)
                            handler_thread = threading.Thread(
                                target=handle_tcp_client,
                                args=(client_socket, client_address, listen_port),
                            )
                            handler_thread.daemon = True
                            handler_thread.start()

                    except socket.error as e:
                        log_error(f"Error aceptando conexión TCP: {e}")

                elif sock.type == socket.SOCK_DGRAM:  # Es un socket de escucha UDP
                    try:
                        data, client_address = sock.recvfrom(BUFFER_SIZE)
                        listen_port, traccar_port = udp_sockets_map[sock]
                        log_debug(
                            f"UDP Datos recibidos de {client_address} en puerto {listen_port} ({len(data)} bytes)"
                        )

                        # Manejar comandos especiales (activate/desactivate) por UDP
                        try:
                            command_str = data.decode("utf-8", errors="ignore").strip()
                            handled_as_command = False
                            if command_str.startswith("desactivate:"):
                                identifier = command_str.split(":", 1)[1]
                                if identifier:
                                    with omit_lock:
                                        omit_identifiers.add(identifier)
                                    log_info(
                                        f"Identificador '{identifier}' añadido a la lista de omisión desde UDP {client_address}"
                                    )
                                    sock.sendto(
                                        b"OK: Desactivated", client_address
                                    )  # Enviar confirmación
                                handled_as_command = True
                            elif command_str.startswith("activate:"):
                                identifier = command_str.split(":", 1)[1]
                                if identifier:
                                    with omit_lock:
                                        omit_identifiers.discard(identifier)
                                    log_info(
                                        f"Identificador '{identifier}' eliminado de la lista de omisión desde UDP {client_address}"
                                    )
                                    sock.sendto(
                                        b"OK: Activated", client_address
                                    )  # Enviar confirmación
                                handled_as_command = True

                            if handled_as_command:
                                continue  # No procesar ni reenviar comandos

                        except Exception as e:
                            log_debug(f"No se pudo decodificar UDP como comando: {e}")
                            pass  # Continuar procesando como datos normales

                        # Verificar si omitir
                        if should_omit(data):
                            continue

                        # Enviar copia a JSON
                        forward_to_json_port(listen_port, data, client_address, "UDP")

                        # Reenviar datos a Traccar vía UDP usando el MISMO socket de escucha
                        # Esto hará que Traccar vea la IP del proxy como origen
                        sock.sendto(data, (TRACCAR_HOST, traccar_port))
                        log_debug(
                            f"UDP Datos de {client_address} reenviados a Traccar {TRACCAR_HOST}:{traccar_port}"
                        )

                        # NOTA: No manejamos respuestas UDP de Traccar de vuelta al cliente aquí.
                        # Traccar respondería (si lo hace) a la IP/puerto del proxy.
                        # Si se necesita bidireccionalidad UDP completa, la solución es más compleja
                        # (requeriría mapeo de `client_address` o usar iptables).

                    except socket.error as e:
                        log_error(f"Error de socket recibiendo/enviando UDP: {e}")
                    except Exception as e:
                        log_error(f"Error inesperado manejando UDP: {e}")

            for sock in exceptional:
                log_error(
                    f"Error excepcional en socket de escucha {sock.getsockname()}. Intentando continuar..."
                )
                # Podríamos intentar cerrar y reabrir el socket aquí, pero es complejo.
                # Por ahora, solo lo reportamos. Si es persistente, requerirá reiniciar.
                # server_sockets.remove(sock) # Ojo: modificar lista mientras se itera es peligroso
                pass

    except KeyboardInterrupt:
        log_info("Cierre solicitado por el usuario (Ctrl+C).")
    finally:
        log_info("Cerrando sockets del servidor...")
        for sock in server_sockets:
            try:
                sock.close()
            except Exception as e:
                log_error(f"Error cerrando socket {sock.getsockname()}: {e}")
        log_info("Servidor detenido.")


# --- Función wrapper para manejar datos iniciales de TCP ---
def handle_tcp_client_with_initial_data(
    client_socket, client_address, listen_port, initial_data
):
    """Similar a handle_tcp_client, pero envía primero initial_data a Traccar."""
    traccar_port = DEVICE_PORTS_MAP.get(listen_port)
    if not traccar_port:
        log_error(f"No se encontró mapeo de puerto Traccar para {listen_port}")
        client_socket.close()
        return

    log_info(
        f"TCP Conexión (con datos iniciales) de {client_address} en puerto {listen_port}, reenviando a {TRACCAR_HOST}:{traccar_port}"
    )
    traccar_socket = None

    try:
        # Conectar a Traccar
        traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        traccar_socket.settimeout(10)
        traccar_socket.connect((TRACCAR_HOST, traccar_port))
        traccar_socket.settimeout(SOCKET_TIMEOUT)
        client_socket.settimeout(SOCKET_TIMEOUT)
        log_debug(f"Conexión TCP establecida con Traccar para {client_address}")

        # Enviar los datos iniciales PRIMERO a Traccar
        source_name_init = f"Cliente {client_address} (inicial)"
        dest_name_init = f"Traccar {TRACCAR_HOST}:{traccar_port}"
        if not should_omit(initial_data):
            forward_to_json_port(listen_port, initial_data, client_address, "TCP")
            traccar_socket.sendall(initial_data)
            log_debug(
                f"{len(initial_data)} bytes iniciales reenviados de {source_name_init} a {dest_name_init}"
            )
        else:
            # Si se omiten los datos iniciales, ¿qué hacemos? ¿Cerramos?
            # Por ahora, continuamos, pero podría ser un punto a revisar.
            log_info(
                f"Datos iniciales omitidos para {client_address}. La conexión puede fallar si Traccar esperaba estos datos."
            )

        # Continuar con el bucle select normal
        sockets = [client_socket, traccar_socket]
        running = True
        while running:
            readable, _, exceptional = select.select(
                sockets, [], sockets, SOCKET_TIMEOUT
            )

            if not readable and not exceptional:
                log_info(
                    f"Timeout de inactividad para TCP {client_address}. Cerrando conexión."
                )
                running = False
                break

            for sock in readable:
                try:
                    data = sock.recv(BUFFER_SIZE)
                    if not data:
                        log_info(
                            f"TCP Conexión cerrada por {'cliente' if sock == client_socket else 'Traccar'} para {client_address}. Cerrando."
                        )
                        running = False
                        break

                    if sock == client_socket:
                        source_name = f"Cliente {client_address}"
                        destination_socket = traccar_socket
                        dest_name = f"Traccar {TRACCAR_HOST}:{traccar_port}"
                        if should_omit(data):
                            continue
                        forward_to_json_port(listen_port, data, client_address, "TCP")
                    else:  # sock == traccar_socket
                        source_name = f"Traccar {TRACCAR_HOST}:{traccar_port}"
                        destination_socket = client_socket
                        dest_name = f"Cliente {client_address}"

                    destination_socket.sendall(data)
                    log_debug(
                        f"{len(data)} bytes reenviados de {source_name} a {dest_name}"
                    )

                except socket.timeout:
                    log_info(
                        f"Socket timeout durante recv/send para TCP {client_address}. Cerrando."
                    )
                    running = False
                    break
                except socket.error as e:
                    log_error(
                        f"Error de socket en TCP {client_address}: {e}. Cerrando."
                    )
                    running = False
                    break
                except Exception as e:
                    log_error(
                        f"Error inesperado manejando TCP {client_address}: {e}. Cerrando."
                    )
                    running = False
                    break

            for sock in exceptional:
                log_error(
                    f"Error excepcional en socket TCP para {client_address}. Cerrando."
                )
                running = False
                break

            if not running:
                break

    except socket.timeout:
        log_error(
            f"Timeout conectando a Traccar {TRACCAR_HOST}:{traccar_port} para cliente {client_address}"
        )
    except socket.error as e:
        log_error(
            f"Error de socket conectando/manejando TCP {client_address} -> Traccar {TRACCAR_HOST}:{traccar_port}: {e}"
        )
    except Exception as e:
        log_error(f"Error general manejando TCP {client_address}: {e}")
    finally:
        if client_socket:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except (socket.error, OSError):
                pass
            try:
                client_socket.close()
            except socket.error:
                pass
        if traccar_socket:
            try:
                traccar_socket.shutdown(socket.SHUT_RDWR)
            except (socket.error, OSError):
                pass
            try:
                traccar_socket.close()
            except socket.error:
                pass
        log_info(
            f"TCP Conexión (con datos iniciales) finalizada para {client_address} en puerto {listen_port}"
        )


# --- Punto de Entrada ---
if __name__ == "__main__":
    start_server()
