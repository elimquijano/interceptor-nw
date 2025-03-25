import socket
import json
import threading
import select
import time
from datetime import datetime
from collections import defaultdict

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

# Diccionario para mantener conexiones UDP persistentes a Traccar
udp_traccar_connections = {}
# Diccionario para almacenar la dirección del cliente por identificador de dispositivo
udp_client_addresses = {}
# Bloqueo para acceso seguro a los diccionarios compartidos
udp_lock = threading.Lock()


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
        # print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Escuchando TCP en el puerto {port}...")

        # Configurar socket UDP
        udp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_server_socket.setblocking(0)
        udp_server_socket.bind(("0.0.0.0", port))
        udp_server_socket.listen(200)
        udp_server_sockets[port] = udp_server_socket
        # print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Escuchando UDP en el puerto {port}...")

    # Lista de todos los sockets de servidor para select
    inputs = list(tcp_server_sockets.values()) + list(udp_server_sockets.values())

    # Iniciar hilo para manejar las conexiones persistentes de UDP a Traccar
    udp_tracker = threading.Thread(target=maintain_udp_connections)
    udp_tracker.daemon = True
    udp_tracker.start()

    # Mantener el servidor funcionando
    try:
        while True:
            # Usar select para monitorear múltiples sockets sin bloquear
            readable, _, exceptional = select.select(inputs, [], inputs, 0.1)

            for sock in readable:
                # Manejar conexiones TCP entrantes
                for port, server_sock in tcp_server_sockets.items():
                    if sock == server_sock:
                        # Nueva conexión TCP entrante
                        client_socket, client_address = server_sock.accept()
                        # print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Conexión TCP aceptada desde {client_address} en el puerto {port}")

                        # Crear un hilo para manejar cada conexión TCP
                        client_handler = threading.Thread(
                            target=handle_device_data, args=(port, client_socket)
                        )
                        client_handler.daemon = True
                        client_handler.start()

                # Manejar datos UDP entrantes
                for port, server_sock in udp_server_sockets.items():
                    if sock == server_sock:
                        # Datos UDP entrantes
                        try:
                            data, client_address = server_sock.recvfrom(1024)
                            """ print(
                                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Datos UDP recibidos desde {client_address} en el puerto {port}"
                            ) """
                            print(f"Datos recibidos: {data}")

                            # Manejar los datos UDP de forma optimizada
                            handle_udp_data(port, data, client_address, server_sock)

                        except Exception as e:
                            """print(
                                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error al recibir datos UDP en puerto {port}: {e}"
                            )"""

            # Verificar sockets con problemas
            for sock in exceptional:
                """print(
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error en socket, cerrando..."
                )"""
                sock.close()
                inputs.remove(sock)

            # Pequeña pausa para evitar consumo excesivo de CPU
            time.sleep(0.01)

    except KeyboardInterrupt:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Cerrando el servidor...")
    finally:
        # Cerrar todos los sockets del servidor
        for port, server_socket in tcp_server_sockets.items():
            server_socket.close()
            """ print(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Socket TCP del servidor en puerto {port} cerrado"
            ) """
        for port, server_socket in udp_server_sockets.items():
            server_socket.close()
            """ print(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Socket UDP del servidor en puerto {port} cerrado"
            ) """

        # Cerrar conexiones persistentes a Traccar
        with udp_lock:
            for key, conn in udp_traccar_connections.items():
                if conn:
                    try:
                        conn.close()
                    except:
                        pass


# Función para mantener las conexiones persistentes de UDP a Traccar
def maintain_udp_connections():
    while True:
        try:
            with udp_lock:
                # Revisar las conexiones existentes
                expired_keys = []
                for key, conn in udp_traccar_connections.items():
                    if not conn:
                        # Crear nueva conexión si no existe
                        try:
                            port = int(key.split("_")[0])
                            traccar_socket = socket.socket(
                                socket.AF_INET, socket.SOCK_STREAM
                            )
                            traccar_socket.connect((TRACCAR_HOST, DEVICE_PORTS[port]))
                            udp_traccar_connections[key] = traccar_socket
                            """ print(
                                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Conexión UDP-TCP persistente establecida para {key}"
                            ) """
                        except Exception as e:
                            """print(
                                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} No se pudo establecer conexión para {key}: {e}"
                            )"""
                    else:
                        # Verificar si la conexión sigue activa
                        try:
                            conn.settimeout(0.1)
                            # Enviar datos de heartbeat si es necesario
                            # (algunos servidores requieren datos periódicos para mantener la conexión)
                            # conn.sendall(b'\r\n') # Usar solo si es necesario
                        except Exception as e:
                            """print(
                                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Conexión caída para {key}: {e}"
                            )"""
                            try:
                                conn.close()
                            except:
                                pass
                            expired_keys.append(key)

                # Eliminar conexiones caídas
                for key in expired_keys:
                    udp_traccar_connections[key] = None
        except Exception as e:
            """print(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error en maintain_udp_connections: {e}"
            )"""

        # Revisar cada 10 segundos
        time.sleep(10)


# Función para generar un identificador único para dispositivos UDP
def get_device_id(port, client_address):
    # Se puede mejorar para extraer un ID verdadero del dispositivo si está disponible en los datos
    return f"{port}_{client_address[0]}_{client_address[1]}"


# Función mejorada para manejar datos UDP
def handle_udp_data(port, data, client_address, udp_server_socket):
    try:
        # Generar un identificador para este dispositivo
        device_id = get_device_id(port, client_address)

        # Enviar los datos al puerto JSON
        send_to_json_port(port, data)

        with udp_lock:
            # Guardar la dirección del cliente para enviar respuestas posteriormente
            udp_client_addresses[device_id] = client_address

            # Verificar si ya existe una conexión para este dispositivo
            if (
                device_id not in udp_traccar_connections
                or not udp_traccar_connections[device_id]
            ):
                # Crear una nueva conexión TCP a Traccar
                traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    traccar_socket.connect((TRACCAR_HOST, DEVICE_PORTS[port]))
                    udp_traccar_connections[device_id] = traccar_socket
                    """ print(
                        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Nueva conexión Traccar establecida para dispositivo UDP {device_id}"
                    ) """
                except socket.error as e:
                    """print(
                        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Conexión rechazada por Traccar en puerto {DEVICE_PORTS[port]} para UDP. Error: {e}"
                    )"""
                    return

            # Usar la conexión existente
            traccar_socket = udp_traccar_connections[device_id]

        # Enviar datos al servidor Traccar
        try:
            traccar_socket.sendall(data)
            """ print(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Datos UDP de {client_address} a Traccar (puerto {DEVICE_PORTS[port]}): {len(data)} bytes"
            ) """
        except Exception as e:
            """print(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error al enviar datos a Traccar: {e}"
            )"""
            # Marcar la conexión como cerrada para que se restablezca
            with udp_lock:
                try:
                    traccar_socket.close()
                except:
                    pass
                udp_traccar_connections[device_id] = None
            return

        # Iniciar un hilo para escuchar respuestas de Traccar
        response_handler = threading.Thread(
            target=listen_for_traccar_response, args=(device_id, udp_server_socket)
        )
        response_handler.daemon = True
        response_handler.start()

    except Exception as e:
        """print(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error general al manejar datos UDP en puerto {port}: {e}"
        )"""


# Función para escuchar respuestas de Traccar para dispositivos UDP
def listen_for_traccar_response(device_id, udp_server_socket):
    with udp_lock:
        if (
            device_id not in udp_traccar_connections
            or not udp_traccar_connections[device_id]
        ):
            return

        traccar_socket = udp_traccar_connections[device_id]
        if device_id not in udp_client_addresses:
            return

        client_address = udp_client_addresses[device_id]

    try:
        # Configurar el socket para no bloquear pero con un timeout
        traccar_socket.setblocking(0)

        # Usar select para esperar datos sin bloquear completamente
        ready, _, _ = select.select([traccar_socket], [], [], 0.5)

        if ready:
            response_data = traccar_socket.recv(1024)
            if response_data:
                # Enviar respuesta de vuelta al cliente UDP
                udp_server_socket.sendto(response_data, client_address)
                """ print(
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Respuesta de Traccar al cliente UDP {client_address}: {len(response_data)} bytes"
                ) """

    except socket.error as e:
        """print(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error de socket al escuchar respuesta Traccar para {device_id}: {e}"
        )"""
        # Marcar la conexión como cerrada para que se restablezca
        with udp_lock:
            try:
                traccar_socket.close()
            except:
                pass
            udp_traccar_connections[device_id] = None
    except Exception as e:
        """print(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error al procesar respuesta de Traccar para UDP {device_id}: {e}"
        )"""


# Función para reenviar datos entre dos sockets (TCP)
def forward_data(
    source_socket, destination_socket, source_name, dest_name, buffer_size=1024
):
    try:
        # Leer datos del socket de origen
        data = source_socket.recv(buffer_size)
        if not data:
            """print(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Conexión cerrada desde {source_name}"
            )"""
            return None

        # Enviar datos al socket de destino
        destination_socket.sendall(data)
        """ print(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} De {source_name} a {dest_name}: {len(data)} bytes"
        ) """

        return data  # Devolver los datos para que puedan usarse con JSON_PORT
    except Exception as e:
        """print(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error de {source_name} a {dest_name}: {e}"
        )"""
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
            """print(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Conexión rechazada por Traccar en puerto {DEVICE_PORTS[port]}. ¿El servidor está ejecutándose? Error: {e}"
            )"""
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
                            # Enviar los datos al puerto JSON
                            send_to_json_port(port, data)
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
                    """print(
                        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error en socket durante comunicación, cerrando..."
                    )"""
                    running = False

            except (socket.error, socket.timeout) as e:
                """print(
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Conexión reiniciada o abortada: {e}"
                )"""
                running = False
            except Exception as e:
                """print(
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error durante la comunicación de datos: {e}"
                )"""
                running = False

    except Exception as e:
        """print(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error al manejar conexión en puerto {port}: {e}"
        )"""

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
        """ print(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Conexión finalizada para dispositivo en puerto {port}"
        ) """


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
        json_socket.settimeout(2)  # Timeout corto para no bloquear

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
        """print(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error al enviar datos al puerto JSON: {e}"
        )"""

    finally:
        # Cerrar el socket
        if json_socket:
            try:
                json_socket.close()
            except:
                pass


# Iniciar el servidor
if __name__ == "__main__":
    listen_for_data()
