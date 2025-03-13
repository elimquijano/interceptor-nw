import socket
import json
import threading
import select
import time
from datetime import datetime

# Puertos de escucha para los dispositivos GPS
DEVICE_PORTS = {
    6013: 5013,  # Sinotrack: 6013 -> Traccar 5013
    6001: 5001,  # Coban: 6001 -> Traccar 5001
    6027: 5027,  # Teltonika: 6027 -> Traccar 5027
}

# Puerto adicional para enviar la data en formato JSON
JSON_PORT = 7005

# Dirección del servidor Traccar
TRACCAR_HOST = 'localhost'

# Función para manejar la conexión y escuchar los datos
def listen_for_data():
    # Crear los sockets TCP y UDP para escuchar en múltiples puertos
    tcp_server_sockets = {}
    udp_server_sockets = {}

    for port in DEVICE_PORTS.keys():
        # Configurar socket TCP
        tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_socket.setblocking(0)
        tcp_socket.bind(('0.0.0.0', port))
        tcp_socket.listen(100)
        tcp_server_sockets[port] = tcp_socket
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Escuchando TCP en el puerto {port}...")

        # Configurar socket UDP
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_socket.setblocking(0)
        udp_socket.bind(('0.0.0.0', port))
        udp_server_sockets[port] = udp_socket
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Escuchando UDP en el puerto {port}...")

    # Lista de todos los sockets de servidor para select
    inputs = list(tcp_server_sockets.values()) + list(udp_server_sockets.values())
    
    # Diccionario para rastrear hilos UDP activos
    udp_threads = {}

    # Mantener el servidor funcionando
    try:
        while True:
            # Usar select para monitorear múltiples sockets sin bloquear
            readable, _, exceptional = select.select(inputs, [], inputs, 0.1)

            for sock in readable:
                # Manejar conexiones TCP entrantes
                for port, tcp_sock in tcp_server_sockets.items():
                    if sock == tcp_sock:
                        # Nueva conexión TCP entrante
                        client_socket, client_address = tcp_sock.accept()
                        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Conexión TCP aceptada desde {client_address} en el puerto {port}")

                        # Crear un hilo para manejar cada conexión TCP
                        client_handler = threading.Thread(
                            target=handle_tcp_device_data,
                            args=(port, client_socket)
                        )
                        client_handler.daemon = True
                        client_handler.start()
                
                # Manejar mensajes UDP entrantes
                for port, udp_sock in udp_server_sockets.items():
                    if sock == udp_sock:
                        try:
                            # Recibir datos UDP y la dirección del cliente
                            data, client_address = udp_sock.recvfrom(4096)
                            
                            if data:
                                client_id = f"{client_address[0]}:{client_address[1]}"
                                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Datos UDP recibidos desde {client_id} en el puerto {port}")
                                
                                # Crear un identificador único para este cliente UDP
                                client_key = f"{port}:{client_id}"
                                
                                # Si ya hay un hilo para este cliente, enviarle los datos
                                if client_key in udp_threads and udp_threads[client_key]['queue']:
                                    udp_threads[client_key]['queue'].append((data, client_address))
                                else:
                                    # Comenzar un nuevo hilo para este cliente UDP
                                    msg_queue = []
                                    msg_queue.append((data, client_address))
                                    
                                    udp_threads[client_key] = {
                                        'queue': msg_queue,
                                        'active': True
                                    }
                                    
                                    udp_handler = threading.Thread(
                                        target=handle_udp_device_data,
                                        args=(port, client_address, msg_queue, client_key, udp_threads)
                                    )
                                    udp_handler.daemon = True
                                    udp_handler.start()
                        except Exception as e:
                            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error al procesar datos UDP en puerto {port}: {e}")

            # Verificar sockets con problemas
            for sock in exceptional:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error en socket, cerrando...")
                sock.close()
                inputs.remove(sock)

            # Limpiar hilos UDP inactivos
            for client_key in list(udp_threads.keys()):
                if not udp_threads[client_key]['active']:
                    del udp_threads[client_key]

            # Pequeña pausa para evitar consumo excesivo de CPU
            time.sleep(0.01)

    except KeyboardInterrupt:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Cerrando el servidor...")
    finally:
        # Cerrar todos los sockets del servidor
        for port, server_socket in tcp_server_sockets.items():
            server_socket.close()
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Socket TCP del servidor en puerto {port} cerrado")
        
        for port, server_socket in udp_server_sockets.items():
            server_socket.close()
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Socket UDP del servidor en puerto {port} cerrado")

# Función para manejar datos UDP
def handle_udp_device_data(port, client_address, msg_queue, client_key, udp_threads):
    udp_socket = None
    try:
        # Crear socket UDP para comunicarse con Traccar
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        traccar_port = DEVICE_PORTS[port]
        
        # Socket para recibir respuestas de Traccar
        response_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        response_socket.bind(('0.0.0.0', 0))  # Puerto aleatorio
        response_socket.settimeout(0.5)  # Timeout corto
        
        # Obtener el puerto asignado para las respuestas
        _, response_port = response_socket.getsockname()
        
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Hilo UDP iniciado para cliente {client_address} en puerto {port} -> {traccar_port}")
        
        # Procesar mensajes en la cola
        while udp_threads[client_key]['active']:
            # Comprobar si hay mensajes en la cola
            if msg_queue:
                data, addr = msg_queue.pop(0)
                
                # Enviar datos a Traccar por UDP
                udp_socket.sendto(data, (TRACCAR_HOST, traccar_port))
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Datos UDP enviados a Traccar (puerto {traccar_port}): {len(data)} bytes")
                
                # Enviar los datos al puerto JSON
                send_to_json_port(port, data, protocol="UDP")
                
                # Intentar recibir respuesta de Traccar
                try:
                    response, _ = response_socket.recvfrom(4096)
                    if response:
                        # Reenviar respuesta al cliente original
                        udp_socket.sendto(response, addr)
                        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Respuesta UDP enviada al cliente: {len(response)} bytes")
                except socket.timeout:
                    pass  # No hay respuesta, continuar
            
            # Esperar un poco antes de procesar el siguiente mensaje
            time.sleep(0.1)
            
            # Si no hay mensajes por un tiempo, considerar inactivo
            if not msg_queue:
                time.sleep(2)  # Esperar un poco más para ver si llegan nuevos mensajes
                if not msg_queue:
                    udp_threads[client_key]['active'] = False
                    break
    
    except Exception as e:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error en el hilo UDP para puerto {port}: {e}")
    
    finally:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Finalizando hilo UDP para cliente {client_address}")
        if udp_socket:
            udp_socket.close()
        if 'response_socket' in locals() and response_socket:
            response_socket.close()
        udp_threads[client_key]['active'] = False

# Función para reenviar datos TCP entre dos sockets
def forward_tcp_data(source_socket, destination_socket, source_name, dest_name, buffer_size=4096):
    try:
        # Leer datos del socket de origen
        data = source_socket.recv(buffer_size)
        if not data:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Conexión cerrada desde {source_name}")
            return None

        # Enviar datos al socket de destino
        destination_socket.sendall(data)
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} De {source_name} a {dest_name}: {len(data)} bytes")

        return data  # Devolver los datos para que puedan usarse con JSON_PORT
    except Exception as e:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error de {source_name} a {dest_name}: {e}")
        return None

# Función para manejar los datos TCP de un dispositivo
def handle_tcp_device_data(port, client_socket):
    traccar_socket = None
    try:
        # Conectar al servidor Traccar para el puerto correspondiente
        traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            traccar_socket.connect((TRACCAR_HOST, DEVICE_PORTS[port]))
        except socket.error as e:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Conexión TCP rechazada por Traccar en puerto {DEVICE_PORTS[port]}. ¿El servidor está ejecutándose? Error: {e}")
            return

        # Configuración bidireccional - permitir respuestas de Traccar al dispositivo
        device_name = f"dispositivo TCP (puerto {port})"
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
                        data = forward_tcp_data(client_socket, traccar_socket, device_name, traccar_name)
                        if data:
                            # Enviar los datos al puerto JSON
                            send_to_json_port(port, data, protocol="TCP")
                        else:
                            running = False
                            break

                    elif sock == traccar_socket:
                        # Datos de Traccar hacia el dispositivo GPS (respuesta)
                        if not forward_tcp_data(traccar_socket, client_socket, traccar_name, device_name):
                            running = False
                            break

                # Verificar sockets con problemas
                for sock in exceptional:
                    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error en socket durante comunicación, cerrando...")
                    running = False

            except (socket.error, socket.timeout) as e:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Conexión reiniciada o abortada: {e}")
                running = False
            except Exception as e:
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error durante la comunicación de datos: {e}")
                running = False

    except Exception as e:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error al manejar conexión TCP en puerto {port}: {e}")

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
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Conexión TCP finalizada para dispositivo en puerto {port}")

# Función para enviar los datos en formato JSON al puerto 7005
def send_to_json_port(port, data, protocol="TCP"):
    json_socket = None
    try:
        # Intentar decodificar los datos como UTF-8, si falla usar hexadecimal
        try:
            decoded_data = data.decode('utf-8', errors='replace')
        except:
            decoded_data = data.hex()

        # Crear un socket para enviar los datos en formato JSON
        json_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        json_socket.settimeout(2)  # Timeout corto para no bloquear

        # Conectar al puerto 7005
        try:
            json_socket.connect(('localhost', JSON_PORT))

            # Crear el diccionario con los datos y puerto
            json_data = {
                "port": port,
                "protocol": protocol,
                "data": decoded_data,
                "timestamp": time.time()
            }

            # Convertir a JSON y enviar
            json_socket.sendall(json.dumps(json_data).encode('utf-8'))
        except socket.error:
            # Silenciosamente fallar si el puerto JSON no está disponible
            pass

    except Exception as e:
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error al enviar datos al puerto JSON: {e}")

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