import socket
import json
import threading
import select
import time
from datetime import datetime
import sys
import os

# Puertos de escucha para los dispositivos GPS
DEVICE_PORTS = {
    6013: 5013,  # Sinotrack: 6013 -> Traccar 5013
    6001: 5001,  # Coban: 6001 -> Traccar 5001
    6027: 5027,  # Teltonika: 6027 -> Traccar 5027
}

# Puerto adicional para enviar la data en formato JSON
JSON_PORT = 7005

# Dirección del servidor Traccar
TRACCAR_HOST = "localhost"

# Límite de conexiones por dispositivo - solo mantener MAX_CONNECTIONS_PER_DEVICE
# conexiones activas por dispositivo identificado
MAX_CONNECTIONS_PER_DEVICE = 5

# Diccionario para almacenar información de dispositivos por IMEI
devices_info = {}
devices_lock = threading.Lock()

# Contador de conexiones activas
active_tcp_connections = 0
active_udp_connections = 0
connections_lock = threading.Lock()

# Diccionario para mantener el estado de los números que deben ser omitidos
omit_numbers = {}

# Tiempo máximo de inactividad para una conexión en segundos
CONNECTION_TIMEOUT = 60


# Función para extraer IMEI o identificador del dispositivo
def extract_device_id(data):
    try:
        # Intentar decodificar los datos
        data_str = data.decode("utf-8", errors="ignore")

        # Buscar patrones comunes en mensajes GPS
        # Sinotrack: *HQ,9175770835,V1,... - buscamos el segundo campo entre comas
        if "*HQ," in data_str:
            parts = data_str.split(",")
            if len(parts) > 2:
                return parts[1]

        # Otros patrones comunes de GPS
        # Coban: ##,imei:123456789012345,A;
        if "imei:" in data_str:
            imei_part = data_str.split("imei:")[1]
            return imei_part.split(",")[0]

        # Teltonika y otros dispositivos
        # Si no hay un patrón específico, usar una parte del mensaje como hash
        return "hash_" + str(hash(data_str) % 10000)
    except:
        # Si no podemos extraer un ID, usar un hash del mensaje
        return "data_" + str(hash(data) % 10000)


# Función para registrar mensajes de log
def log_message(message, log_type="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} - {log_type} - {message}")


# Función para manejar la conexión y escuchar los datos (TCP y UDP)
def listen_for_data():
    # Crear sockets TCP para escuchar en múltiples puertos
    tcp_server_sockets = {}
    # Crear sockets UDP para escuchar en los mismos puertos
    udp_server_sockets = {}

    # Configurar los sockets para cada puerto
    for port in DEVICE_PORTS.keys():
        # Configurar socket TCP
        tcp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_server_socket.setblocking(0)
        tcp_server_socket.bind(("0.0.0.0", port))
        tcp_server_socket.listen(200)
        tcp_server_sockets[port] = tcp_server_socket
        log_message(f"Escuchando TCP en el puerto {port}...")

        # Configurar socket UDP
        udp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_server_socket.setblocking(0)
        udp_server_socket.bind(("0.0.0.0", port))
        udp_server_sockets[port] = udp_server_socket
        log_message(f"Escuchando UDP en el puerto {port}...")

    # Lista de todos los sockets de servidor para select
    inputs = list(tcp_server_sockets.values()) + list(udp_server_sockets.values())

    # Iniciar el thread de limpieza
    cleanup_thread = threading.Thread(target=cleanup_inactive_connections)
    cleanup_thread.daemon = True
    cleanup_thread.start()

    # Thread para mostrar estadísticas
    stats_thread = threading.Thread(target=show_connection_stats)
    stats_thread.daemon = True
    stats_thread.start()

    # Mantener el servidor funcionando
    try:
        while True:
            try:
                # Usar select para monitorear múltiples sockets sin bloquear
                readable, _, exceptional = select.select(inputs, [], inputs, 0.1)

                for sock in readable:
                    # Manejar conexiones TCP entrantes
                    is_tcp = False
                    for port, server_sock in tcp_server_sockets.items():
                        if sock == server_sock:
                            is_tcp = True
                            # Nueva conexión TCP entrante
                            try:
                                client_socket, client_address = server_sock.accept()
                                # Manejar cada conexión TCP en un hilo separado
                                client_handler = threading.Thread(
                                    target=handle_tcp_connection,
                                    args=(port, client_socket, client_address),
                                )
                                client_handler.daemon = True
                                client_handler.start()
                            except Exception as e:
                                log_message(
                                    f"Error al aceptar conexión TCP en puerto {port}: {e}",
                                    "ERROR",
                                )

                    # Manejar datos UDP entrantes si no era un socket TCP
                    if not is_tcp:
                        for port, server_sock in udp_server_sockets.items():
                            if sock == server_sock:
                                try:
                                    data, client_address = server_sock.recvfrom(1024)

                                    # Verificar comandos de control
                                    try:
                                        command = data.decode("utf-8", errors="ignore")
                                        if command.startswith("desactivate:"):
                                            number = command.split(":")[1]
                                            omit_numbers[number] = True
                                            log_message(f"Número {number} desactivado.")
                                            continue
                                        elif command.startswith("activate:"):
                                            number = command.split(":")[1]
                                            if number in omit_numbers:
                                                del omit_numbers[number]
                                            log_message(f"Número {number} activado.")
                                            continue
                                    except:
                                        pass

                                    # Manejar datos UDP
                                    udp_handler = threading.Thread(
                                        target=handle_udp_data,
                                        args=(port, data, client_address, server_sock),
                                    )
                                    udp_handler.daemon = True
                                    udp_handler.start()
                                except Exception as e:
                                    log_message(
                                        f"Error al recibir datos UDP en puerto {port}: {e}",
                                        "ERROR",
                                    )

                # Verificar sockets con problemas
                for sock in exceptional:
                    log_message(f"Error en socket, cerrando...", "WARNING")
                    sock.close()
                    inputs.remove(sock)

                # Pequeña pausa para evitar consumo excesivo de CPU
                time.sleep(0.01)

            except (select.error, socket.error) as e:
                log_message(f"Error en select: {e}", "ERROR")
                # Reconstruir lista de sockets si es necesario
                inputs = list(tcp_server_sockets.values()) + list(
                    udp_server_sockets.values()
                )
                time.sleep(1)
            except Exception as e:
                log_message(f"Error general en bucle principal: {e}", "ERROR")
                time.sleep(1)

    except KeyboardInterrupt:
        log_message("Cerrando el servidor por interrupción del usuario...")
    finally:
        # Cerrar todos los sockets del servidor
        for port, server_socket in tcp_server_sockets.items():
            server_socket.close()
            log_message(f"Socket TCP del servidor en puerto {port} cerrado")
        for port, server_socket in udp_server_sockets.items():
            server_socket.close()
            log_message(f"Socket UDP del servidor en puerto {port} cerrado")


# Función para manejar las conexiones TCP
def handle_tcp_connection(port, client_socket, client_address):
    global active_tcp_connections

    # Incrementar contador de conexiones
    with connections_lock:
        active_tcp_connections += 1

    traccar_socket = None
    device_id = None

    try:
        # Establecer timeout para evitar conexiones colgadas
        client_socket.settimeout(CONNECTION_TIMEOUT)

        # Esperar los primeros datos para identificar el dispositivo
        data = client_socket.recv(1024)
        if not data:
            return

        # Extraer identificador del dispositivo
        device_id = extract_device_id(data)

        # Verificar si el número está en la lista de omitidos
        decoded_data = data.decode("utf-8", errors="ignore")
        for number in omit_numbers:
            if number in decoded_data and "tracker" in decoded_data:
                log_message(f"Datos TCP omitidos para el número {number}.")
                return

        # Enviar datos al puerto JSON
        send_to_json_port(port, data)

        # Conectar al servidor Traccar
        try:
            traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            traccar_socket.settimeout(CONNECTION_TIMEOUT)
            traccar_socket.connect((TRACCAR_HOST, DEVICE_PORTS[port]))

            # Enviar los datos iniciales a Traccar
            traccar_socket.sendall(data)

            # Esperar la respuesta de Traccar (si hay)
            traccar_socket.settimeout(2)  # Timeout corto para respuesta
            try:
                response = traccar_socket.recv(1024)
                if response:
                    # Enviar respuesta al dispositivo
                    client_socket.sendall(response)
            except socket.timeout:
                # No hay respuesta, continuar
                pass

        except Exception as e:
            log_message(
                f"Error al conectar con Traccar (puerto {DEVICE_PORTS[port]}): {e}",
                "ERROR",
            )
            return

        # Configurar para lectura con timeout
        client_socket.settimeout(CONNECTION_TIMEOUT)
        traccar_socket.settimeout(CONNECTION_TIMEOUT)

        # Bucle de comunicación
        while True:
            try:
                # Verificar si hay más datos del cliente
                readable, _, _ = select.select([client_socket], [], [], 1)
                if client_socket in readable:
                    data = client_socket.recv(1024)
                    if not data:
                        break

                    # Enviar datos a Traccar
                    traccar_socket.sendall(data)

                    # Enviar al puerto JSON
                    send_to_json_port(port, data)

                    # Verificar respuesta de Traccar
                    traccar_socket.settimeout(2)
                    try:
                        response = traccar_socket.recv(1024)
                        if response:
                            client_socket.sendall(response)
                    except socket.timeout:
                        # No hay respuesta, continuar
                        pass
                else:
                    # No hay nuevos datos, salir del bucle
                    break

            except (socket.timeout, socket.error):
                # Timeout o error de socket, terminar la conexión
                break
            except Exception as e:
                log_message(f"Error en la comunicación TCP: {e}", "ERROR")
                break

    except Exception as e:
        log_message(f"Error al manejar conexión TCP en puerto {port}: {e}", "ERROR")

    finally:
        # Cerrar las conexiones
        if client_socket:
            try:
                client_socket.close()
            except:
                pass
        if traccar_socket:
            try:
                traccar_socket.close()
            except:
                pass

        # Decrementar contador de conexiones
        with connections_lock:
            active_tcp_connections -= 1


# Función para manejar los datos UDP
def handle_udp_data(port, data, client_address, udp_server_socket):
    global active_udp_connections

    # Incrementar contador de conexiones
    with connections_lock:
        active_udp_connections += 1

    try:
        # Extraer identificador del dispositivo
        device_id = extract_device_id(data)

        # Verificar si el número está en la lista de omitidos
        decoded_data = data.decode("utf-8", errors="ignore")
        for number in omit_numbers:
            if number in decoded_data and "tracker" in decoded_data:
                log_message(f"Datos UDP omitidos para el número {number}.")
                return

        # Enviar datos al puerto JSON
        send_to_json_port(port, data)

        # Crear socket para enviar a Traccar (una conexión nueva para cada mensaje)
        traccar_socket = None
        try:
            traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            traccar_socket.settimeout(5)  # Timeout de 5 segundos
            traccar_socket.connect((TRACCAR_HOST, DEVICE_PORTS[port]))

            # Enviar datos a Traccar
            traccar_socket.sendall(data)

            # Esperar respuesta de Traccar
            try:
                traccar_socket.settimeout(2)  # Timeout corto para la respuesta
                response = traccar_socket.recv(1024)
                if response:
                    # Enviar respuesta de vuelta al cliente UDP
                    udp_server_socket.sendto(response, client_address)
            except socket.timeout:
                # No hay respuesta, continuar
                pass

        except Exception as e:
            log_message(
                f"Error al comunicarse con Traccar para datos UDP: {e}", "ERROR"
            )

        finally:
            # Cerrar socket de Traccar
            if traccar_socket:
                try:
                    traccar_socket.close()
                except:
                    pass

    except Exception as e:
        log_message(f"Error al manejar datos UDP: {e}", "ERROR")

    finally:
        # Decrementar contador de conexiones
        with connections_lock:
            active_udp_connections -= 1


# Función para enviar los datos en formato JSON al puerto 7005
def send_to_json_port(port, data):
    json_socket = None
    try:
        # Intentar decodificar los datos como UTF-8, si falla usar hexadecimal
        try:
            decoded_data = data.decode("utf-8", errors="replace")
        except:
            decoded_data = data.hex()

        # Crear un socket para enviar los datos en formato JSON
        json_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        json_socket.settimeout(1)  # Timeout corto para no bloquear

        # Conectar al puerto JSON
        try:
            json_socket.connect(("localhost", JSON_PORT))

            # Crear el diccionario con los datos y puerto
            json_data = {"port": port, "data": decoded_data, "timestamp": time.time()}

            # Convertir a JSON y enviar
            json_socket.sendall(json.dumps(json_data).encode("utf-8"))
        except socket.error:
            # Silenciosamente fallar si el puerto JSON no está disponible
            pass

    except Exception as e:
        log_message(f"Error al enviar datos al puerto JSON: {e}", "ERROR")

    finally:
        # Cerrar el socket
        if json_socket:
            try:
                json_socket.close()
            except:
                pass


# Función para monitorear y mostrar estadísticas de conexiones
def show_connection_stats():
    while True:
        try:
            with connections_lock:
                tcp_count = active_tcp_connections
                udp_count = active_udp_connections

            log_message(f"Conexiones TCP activas: {tcp_count}")
            log_message(f"Conexiones UDP activas: {udp_count}")

            # Verificar si hay demasiadas conexiones
            if tcp_count > 1000 or udp_count > 1000:
                log_message(
                    f"¡ADVERTENCIA! Número elevado de conexiones activas. Posible fuga de recursos.",
                    "WARNING",
                )

            # Verificar uso de file descriptors
            try:
                # Esta parte es específica de Linux/Unix
                if os.path.exists("/proc/self/fd"):
                    num_fds = len(os.listdir("/proc/self/fd"))
                    log_message(f"File descriptors en uso: {num_fds}")

                    # Advertir si se acerca al límite
                    soft_limit, hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
                    if num_fds > soft_limit * 0.8:
                        log_message(
                            f"¡ADVERTENCIA! Uso alto de file descriptors: {num_fds}/{soft_limit}",
                            "WARNING",
                        )
            except Exception:
                # No hacer nada si no se puede obtener esta información
                pass

        except Exception as e:
            log_message(f"Error al mostrar estadísticas: {e}", "ERROR")

        # Mostrar cada 60 segundos
        time.sleep(60)


# Función para limpiar conexiones inactivas y recursos
def cleanup_inactive_connections():
    while True:
        try:
            # Mostrar uso de memoria
            try:
                import psutil

                process = psutil.Process(os.getpid())
                memory_info = process.memory_info()
                log_message(f"Uso de memoria: {memory_info.rss / (1024 * 1024):.2f} MB")
            except ImportError:
                # psutil no está disponible
                pass

        except Exception as e:
            log_message(f"Error en limpieza de conexiones: {e}", "ERROR")

        # Esperar antes de la siguiente limpieza
        time.sleep(300)  # Limpiar cada 5 minutos


# Iniciar el servidor con manejo de errores
if __name__ == "__main__":
    # Intentar importar módulos opcionales
    try:
        import resource

        # Aumentar límite de file descriptors
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (65535, hard))
        log_message(f"Límite de file descriptors establecido a {65535}")
    except ImportError:
        log_message(
            "Módulo 'resource' no disponible. No se puede ajustar el límite de file descriptors",
            "WARNING",
        )

    # Configurar manejo de señales para terminar limpiamente
    import signal

    def signal_handler(sig, frame):
        log_message("Señal de terminación recibida. Cerrando el servidor...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Iniciar el servidor
    try:
        log_message("Iniciando el servidor Interceptor Traccar...")
        listen_for_data()
    except Exception as e:
        log_message(f"Error crítico al iniciar el servidor: {e}", "ERROR")
