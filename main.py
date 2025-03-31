import socket
import json
import threading
import select
import time
from datetime import datetime
from collections import defaultdict, deque
import logging

# Configuración de logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG
)

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

# Estructura para mantener conexiones UDP persistentes a Traccar
# Usamos un diccionario anidado para mejor organización
udp_connections = {
    "sockets": {},  # Almacena socket TCP conectado a Traccar
    "addresses": {},  # Almacena dirección del cliente UDP
    "last_active": {},  # Timestamp de última actividad
    "message_queue": {},  # Cola de mensajes para cada conexión
}

# Bloqueo para acceso seguro a los diccionarios compartidos
udp_lock = threading.Lock()

# Diccionario para mantener el estado de los números que deben ser omitidos
omit_numbers = {}

# Tiempo máximo de inactividad antes de cerrar una conexión (segundos)
MAX_IDLE_TIME = 300  # 5 minutos

# Tamaño máximo de la cola de mensajes por conexión
MAX_QUEUE_SIZE = 20


# Función para manejar la conexión y escuchar los datos (TCP y UDP)
def listen_for_data():
    # Crear sockets TCP para escuchar en múltiples puertos
    tcp_server_sockets = {}
    # Crear sockets UDP para escuchar en los mismos puertos
    udp_server_sockets = {}

    for port in DEVICE_PORTS.keys():
        # Configurar socket TCP
        tcp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_server_socket.setblocking(0)
        tcp_server_socket.bind(("0.0.0.0", port))
        tcp_server_socket.listen(200)
        tcp_server_sockets[port] = tcp_server_socket
        logging.info(f"Escuchando TCP en el puerto {port}...")

        # Configurar socket UDP
        udp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_server_socket.setblocking(0)
        udp_server_socket.bind(("0.0.0.0", port))
        udp_server_sockets[port] = udp_server_socket
        logging.info(f"Escuchando UDP en el puerto {port}...")

    # Lista de todos los sockets de servidor para select
    inputs = list(tcp_server_sockets.values()) + list(udp_server_sockets.values())

    # Lista de sockets que esperan escritura (para respuestas no bloqueantes)
    outputs = []

    # Diccionario para mensajes pendientes
    message_queues = {}

    # Iniciar hilo para limpieza de conexiones inactivas
    cleaner = threading.Thread(target=cleanup_idle_connections)
    cleaner.daemon = True
    cleaner.start()

    # Iniciar el procesador de mensajes para conexiones UDP
    message_processor = threading.Thread(target=process_message_queues)
    message_processor.daemon = True
    message_processor.start()

    # Mantener el servidor funcionando
    try:
        while True:
            # Usar select para monitorear múltiples sockets sin bloquear
            readable, writable, exceptional = select.select(
                inputs + list(udp_connections["sockets"].values()),
                outputs,
                inputs + list(udp_connections["sockets"].values()),
                0.1,
            )

            for sock in readable:
                # Manejar conexiones TCP entrantes
                tcp_handled = False
                for port, server_sock in tcp_server_sockets.items():
                    if sock == server_sock:
                        tcp_handled = True
                        # Nueva conexión TCP entrante
                        client_socket, client_address = server_sock.accept()
                        logging.debug(
                            f"Conexión TCP aceptada desde {client_address} en el puerto {port}"
                        )

                        # Crear un hilo para manejar cada conexión TCP
                        client_handler = threading.Thread(
                            target=handle_device_data, args=(port, client_socket)
                        )
                        client_handler.daemon = True
                        client_handler.start()

                # Manejar datos UDP entrantes
                udp_handled = False
                for port, server_sock in udp_server_sockets.items():
                    if sock == server_sock:
                        udp_handled = True
                        # Datos UDP entrantes
                        try:
                            data, client_address = server_sock.recvfrom(1024)
                            logging.debug(
                                f"Datos UDP recibidos desde {client_address} en el puerto {port}"
                            )

                            # Verificar si el comando es para desactivar o activar un número
                            command = data.decode("utf-8", errors="ignore")
                            if command.startswith("desactivate:"):
                                number = command.split(":")[1]
                                omit_numbers[number] = True
                                logging.info(f"Número {number} desactivado.")
                                continue
                            elif command.startswith("activate:"):
                                number = command.split(":")[1]
                                if number in omit_numbers:
                                    del omit_numbers[number]
                                logging.info(f"Número {number} activado.")
                                continue

                            # Manejar los datos UDP de forma no bloqueante
                            handle_udp_data(port, data, client_address, server_sock)

                        except Exception as e:
                            logging.error(
                                f"Error al recibir datos UDP en puerto {port}: {e}"
                            )

                # Manejar respuestas de Traccar para UDP (completamente no bloqueante)
                if not tcp_handled and not udp_handled:
                    # Este socket debe ser una conexión Traccar
                    for device_id, traccar_socket in udp_connections["sockets"].items():
                        if sock == traccar_socket:
                            try:
                                response_data = sock.recv(1024)
                                if response_data:
                                    # Actualizar timestamp de actividad
                                    with udp_lock:
                                        udp_connections["last_active"][
                                            device_id
                                        ] = time.time()

                                        # Obtener la dirección del cliente UDP
                                        if device_id in udp_connections["addresses"]:
                                            client_address = udp_connections[
                                                "addresses"
                                            ][device_id]
                                            port = int(device_id.split("_")[0])

                                            # Enviar respuesta directamente (no bloqueante)
                                            for (
                                                server_sock
                                            ) in udp_server_sockets.values():
                                                try:
                                                    server_sock.sendto(
                                                        response_data, client_address
                                                    )
                                                    logging.debug(
                                                        f"Respuesta de Traccar al cliente UDP {client_address}: {len(response_data)} bytes"
                                                    )
                                                    break
                                                except:
                                                    continue
                                else:
                                    # Conexión cerrada por Traccar
                                    logging.debug(
                                        f"Conexión cerrada por Traccar para {device_id}"
                                    )
                                    with udp_lock:
                                        if device_id in udp_connections["sockets"]:
                                            try:
                                                udp_connections["sockets"][
                                                    device_id
                                                ].close()
                                            except:
                                                pass
                                            del udp_connections["sockets"][device_id]
                            except Exception as e:
                                logging.error(
                                    f"Error al leer respuesta de Traccar para {device_id}: {e}"
                                )
                                with udp_lock:
                                    if device_id in udp_connections["sockets"]:
                                        try:
                                            udp_connections["sockets"][
                                                device_id
                                            ].close()
                                        except:
                                            pass
                                        del udp_connections["sockets"][device_id]

            # Verificar sockets con problemas
            for sock in exceptional:
                logging.error(f"Error en socket, cerrando...")

                # Cerrar socket de servidor si es necesario
                for port, server_sock in tcp_server_sockets.items():
                    if sock == server_sock:
                        sock.close()
                        # Recrear socket
                        new_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        new_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        new_sock.setblocking(0)
                        new_sock.bind(("0.0.0.0", port))
                        new_sock.listen(200)
                        tcp_server_sockets[port] = new_sock
                        inputs.remove(sock)
                        inputs.append(new_sock)
                        logging.info(
                            f"Socket TCP del servidor en puerto {port} recreado"
                        )

                for port, server_sock in udp_server_sockets.items():
                    if sock == server_sock:
                        sock.close()
                        # Recrear socket
                        new_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                        new_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        new_sock.setblocking(0)
                        new_sock.bind(("0.0.0.0", port))
                        udp_server_sockets[port] = new_sock
                        inputs.remove(sock)
                        inputs.append(new_sock)
                        logging.info(
                            f"Socket UDP del servidor en puerto {port} recreado"
                        )

                # Verificar si es un socket Traccar
                for device_id, traccar_socket in list(
                    udp_connections["sockets"].items()
                ):
                    if sock == traccar_socket:
                        with udp_lock:
                            try:
                                sock.close()
                            except:
                                pass
                            if device_id in udp_connections["sockets"]:
                                del udp_connections["sockets"][device_id]
                            logging.debug(
                                f"Socket Traccar para {device_id} cerrado por error"
                            )

            # Pequeña pausa para evitar consumo excesivo de CPU
            time.sleep(0.005)

    except KeyboardInterrupt:
        logging.info("Cerrando el servidor...")
    finally:
        # Cerrar todos los sockets del servidor
        for port, server_socket in tcp_server_sockets.items():
            server_socket.close()
            logging.info(f"Socket TCP del servidor en puerto {port} cerrado")
        for port, server_socket in udp_server_sockets.items():
            server_socket.close()
            logging.info(f"Socket UDP del servidor en puerto {port} cerrado")

        # Cerrar conexiones persistentes a Traccar
        with udp_lock:
            for device_id, sock in udp_connections["sockets"].items():
                if sock:
                    try:
                        sock.close()
                    except:
                        pass


# Función para limpiar conexiones inactivas periódicamente
def cleanup_idle_connections():
    while True:
        try:
            current_time = time.time()
            to_remove = []

            with udp_lock:
                # Identificar conexiones inactivas
                for device_id, last_active in udp_connections["last_active"].items():
                    if current_time - last_active > MAX_IDLE_TIME:
                        to_remove.append(device_id)

                # Eliminar conexiones inactivas
                for device_id in to_remove:
                    if (
                        device_id in udp_connections["sockets"]
                        and udp_connections["sockets"][device_id]
                    ):
                        try:
                            udp_connections["sockets"][device_id].close()
                        except:
                            pass
                        del udp_connections["sockets"][device_id]

                    if device_id in udp_connections["addresses"]:
                        del udp_connections["addresses"][device_id]

                    if device_id in udp_connections["last_active"]:
                        del udp_connections["last_active"][device_id]

                    if device_id in udp_connections["message_queue"]:
                        del udp_connections["message_queue"][device_id]

                    logging.info(f"Conexión inactiva eliminada para {device_id}")

        except Exception as e:
            logging.error(f"Error en cleanup_idle_connections: {e}")

        # Revisar cada 60 segundos
        time.sleep(60)


# Función para procesar las colas de mensajes
def process_message_queues():
    while True:
        try:
            with udp_lock:
                # Procesar mensajes en cola para cada dispositivo
                for device_id, queue in list(udp_connections["message_queue"].items()):
                    if not queue:
                        continue

                    # Verificar si tenemos una conexión activa
                    if (
                        device_id not in udp_connections["sockets"]
                        or not udp_connections["sockets"][device_id]
                    ):
                        # Intentar establecer conexión
                        try:
                            port = int(device_id.split("_")[0])
                            traccar_socket = socket.socket(
                                socket.AF_INET, socket.SOCK_STREAM
                            )
                            traccar_socket.connect((TRACCAR_HOST, DEVICE_PORTS[port]))
                            traccar_socket.setblocking(
                                0
                            )  # Configurar como no bloqueante
                            udp_connections["sockets"][device_id] = traccar_socket
                            logging.debug(f"Conexión establecida para {device_id}")
                        except Exception as e:
                            logging.error(
                                f"No se pudo establecer conexión para {device_id}: {e}"
                            )
                            continue

                    # Intentar enviar el mensaje más antiguo
                    sock = udp_connections["sockets"][device_id]
                    data = queue[0]

                    try:
                        sock.sendall(data)
                        # Mensaje enviado, eliminar de la cola
                        queue.popleft()
                        udp_connections["last_active"][device_id] = time.time()
                        logging.debug(
                            f"Mensaje enviado desde cola para {device_id}, {len(queue)} pendientes"
                        )
                    except socket.error as e:
                        # Socket no listo para escritura, intentar más tarde
                        if e.errno == socket.EAGAIN or e.errno == socket.EWOULDBLOCK:
                            pass
                        else:
                            # Error en socket, cerrar y reintentar después
                            logging.error(
                                f"Error al enviar mensaje desde cola para {device_id}: {e}"
                            )
                            try:
                                sock.close()
                            except:
                                pass
                            udp_connections["sockets"][device_id] = None

        except Exception as e:
            logging.error(f"Error en process_message_queues: {e}")

        # Procesar rápidamente sin consumir mucha CPU
        time.sleep(0.01)


# Función para generar un identificador único para dispositivos UDP
def get_device_id(port, client_address):
    return f"{port}_{client_address[0]}_{client_address[1]}"


# Función mejorada para manejar datos UDP de forma no bloqueante
def handle_udp_data(port, data, client_address, udp_server_socket):
    try:
        # Generar un identificador para este dispositivo
        device_id = get_device_id(port, client_address)

        # Verificar si el número está en la lista de omitidos y si contiene "tracker"
        decoded_data = data.decode("utf-8", errors="ignore")
        for number in omit_numbers:
            if number in decoded_data and "tracker" in decoded_data:
                logging.info(f"Datos omitidos para el número {number}.")
                return

        # Enviar los datos al puerto JSON (no bloqueante)
        try:
            threading.Thread(
                target=send_to_json_port, args=(port, data), daemon=True
            ).start()
        except:
            pass

        with udp_lock:
            # Actualizar la dirección del cliente
            udp_connections["addresses"][device_id] = client_address
            udp_connections["last_active"][device_id] = time.time()

            # Inicializar cola de mensajes si no existe
            if device_id not in udp_connections["message_queue"]:
                udp_connections["message_queue"][device_id] = deque(
                    maxlen=MAX_QUEUE_SIZE
                )

            # Añadir mensaje a la cola
            udp_connections["message_queue"][device_id].append(data)

            # Verificar si podemos enviar inmediatamente (socket existente y no bloqueado)
            if (
                device_id in udp_connections["sockets"]
                and udp_connections["sockets"][device_id]
            ):
                try:
                    # Intentar enviar sin bloquear
                    sock = udp_connections["sockets"][device_id]
                    sock.sendall(data)
                    # Si éxito, eliminar de la cola
                    udp_connections["message_queue"][device_id].popleft()
                    logging.debug(
                        f"Datos UDP enviados directamente a Traccar para {device_id}: {len(data)} bytes"
                    )
                except socket.error as e:
                    if e.errno != socket.EAGAIN and e.errno != socket.EWOULDBLOCK:
                        # Error grave, cerrar socket
                        logging.error(f"Error al enviar datos a Traccar: {e}")
                        try:
                            sock.close()
                        except:
                            pass
                        udp_connections["sockets"][device_id] = None
                except Exception as e:
                    logging.error(f"Error general al enviar datos a Traccar: {e}")

    except Exception as e:
        logging.error(f"Error general al manejar datos UDP en puerto {port}: {e}")


# Función para reenviar datos entre dos sockets (TCP) de forma no bloqueante
def forward_data(
    source_socket, destination_socket, source_name, dest_name, buffer_size=1024
):
    try:
        # Leer datos del socket de origen
        data = source_socket.recv(buffer_size)
        if not data:
            logging.debug(f"Conexión cerrada desde {source_name}")
            return None

        # Enviar datos al socket de destino
        destination_socket.sendall(data)
        logging.debug(f"De {source_name} a {dest_name}: {len(data)} bytes")

        return data  # Devolver los datos para que puedan usarse con JSON_PORT
    except Exception as e:
        logging.error(f"Error de {source_name} a {dest_name}: {e}")
        return None


# Función para manejar los datos de un dispositivo (TCP)
def handle_device_data(port, client_socket):
    traccar_socket = None
    try:
        # Conectar al servidor Traccar para el puerto correspondiente
        traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            traccar_socket.connect((TRACCAR_HOST, DEVICE_PORTS[port]))
        except socket.error as e:
            logging.error(
                f"Conexión rechazada por Traccar en puerto {DEVICE_PORTS[port]}. ¿El servidor está ejecutándose? Error: {e}"
            )
            return

        # Configuración bidireccional - permitir respuestas de Traccar al dispositivo
        device_name = f"dispositivo (puerto {port})"
        traccar_name = f"Traccar (puerto {DEVICE_PORTS[port]})"

        # Configurar sockets como no bloqueantes para select
        client_socket.setblocking(0)
        traccar_socket.setblocking(0)

        # Monitorear ambos sockets
        inputs = [client_socket, traccar_socket]
        running = True

        while running:
            try:
                readable, _, exceptional = select.select(inputs, [], inputs, 1)

                for sock in readable:
                    if sock == client_socket:
                        # Datos del dispositivo GPS hacia Traccar
                        data = forward_data(
                            client_socket, traccar_socket, device_name, traccar_name
                        )
                        if data:
                            # Enviar los datos al puerto JSON en un hilo separado
                            threading.Thread(
                                target=send_to_json_port, args=(port, data), daemon=True
                            ).start()
                        else:
                            running = False
                            break

                    elif sock == traccar_socket:
                        # Datos de Traccar hacia el dispositivo GPS (respuesta)
                        if not forward_data(
                            traccar_socket, client_socket, traccar_name, device_name
                        ):
                            running = False
                            break

                # Verificar sockets con problemas
                for sock in exceptional:
                    logging.error(f"Error en socket durante comunicación, cerrando...")
                    running = False

            except (socket.error, socket.timeout) as e:
                logging.error(f"Conexión reiniciada o abortada: {e}")
                running = False
            except Exception as e:
                logging.error(f"Error durante la comunicación de datos: {e}")
                running = False

    except Exception as e:
        logging.error(f"Error al manejar conexión en puerto {port}: {e}")

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
        logging.debug(f"Conexión finalizada para dispositivo en puerto {port}")


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

        # Conectar al puerto 7005
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
        logging.error(f"Error al enviar datos al puerto JSON: {e}")

    finally:
        # Cerrar el socket
        if json_socket:
            try:
                json_socket.close()
            except:
                pass


# Iniciar el servidor
if __name__ == "__main__":
    try:
        listen_for_data()
    except Exception as e:
        logging.critical(f"Error fatal en el servidor: {e}")
        # Esperar un momento antes de salir para permitir que se registre el error
        time.sleep(1)
