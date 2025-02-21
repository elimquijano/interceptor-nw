import socket
import json
import threading
import select
import time

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
    # Crear el socket TCP para escuchar en múltiples puertos
    server_sockets = {}
    
    for port in DEVICE_PORTS.keys():
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Permitir reutilización de direcciones
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # Configurar como no bloqueante
        server_socket.setblocking(0)
        server_socket.bind(('0.0.0.0', port))
        # Aumentar el backlog a un valor alto para que nunca rechace conexiones
        # pero mantenerlo razonable para evitar problemas de rendimiento
        server_socket.listen(100)
        server_sockets[port] = server_socket
        print(f"Escuchando en el puerto {port}...")
    
    # Lista de todos los sockets de servidor para select
    inputs = list(server_sockets.values())
    
    # Mantener el servidor funcionando
    try:
        while True:
            # Usar select para monitorear múltiples sockets sin bloquear
            readable, _, exceptional = select.select(inputs, [], inputs, 0.1)
            
            for sock in readable:
                for port, server_sock in server_sockets.items():
                    if sock == server_sock:
                        # Nueva conexión entrante
                        client_socket, client_address = server_sock.accept()
                        print(f"Conexión aceptada desde {client_address} en el puerto {port}")
                        
                        # Crear un hilo para manejar cada conexión
                        client_handler = threading.Thread(
                            target=handle_device_data, 
                            args=(port, client_socket)
                        )
                        # Configurar como daemon para que el hilo se cierre cuando se cierre el programa principal
                        client_handler.daemon = True
                        client_handler.start()
            
            # Verificar sockets con problemas
            for sock in exceptional:
                print(f"Error en socket, cerrando...")
                sock.close()
                inputs.remove(sock)
            
            # Pequeña pausa para evitar consumo excesivo de CPU
            time.sleep(0.01)
    
    except KeyboardInterrupt:
        print("Cerrando el servidor...")
    finally:
        # Cerrar todos los sockets del servidor
        for port, server_socket in server_sockets.items():
            server_socket.close()
            print(f"Socket del servidor en puerto {port} cerrado")

# Función para reenviar datos entre dos sockets
def forward_data(source_socket, destination_socket, source_name, dest_name, buffer_size=1024):
    try:
        while True:
            # Leer datos del socket de origen
            data = source_socket.recv(buffer_size)
            if not data:
                print(f"Conexión cerrada desde {source_name}")
                break
                
            # Enviar datos al socket de destino
            destination_socket.sendall(data)
            print(f"Datos reenviados de {source_name} a {dest_name}: {len(data)} bytes")
            
            return data  # Devolver los datos para que puedan usarse con JSON_PORT
    except Exception as e:
        print(f"Error al reenviar datos de {source_name} a {dest_name}: {e}")
        return None

# Función para manejar los datos de un dispositivo
def handle_device_data(port, client_socket):
    traccar_socket = None
    try:
        # Conectar al servidor Traccar para el puerto correspondiente
        traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            traccar_socket.connect((TRACCAR_HOST, DEVICE_PORTS[port]))
        except ConnectionRefusedError:
            print(f"Conexión rechazada por Traccar en puerto {DEVICE_PORTS[port]}. ¿El servidor está ejecutándose?")
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
                        data = forward_data(client_socket, traccar_socket, device_name, traccar_name)
                        if data:
                            # Enviar los datos al puerto JSON
                            send_to_json_port(port, data)
                        else:
                            running = False
                            break
                            
                    elif sock == traccar_socket:
                        # Datos de Traccar hacia el dispositivo GPS (respuesta)
                        if not forward_data(traccar_socket, client_socket, traccar_name, device_name):
                            running = False
                            break
                
                # Verificar sockets con problemas
                for sock in exceptional:
                    print(f"Error en socket durante comunicación, cerrando...")
                    running = False
            
            except (ConnectionResetError, ConnectionAbortedError) as e:
                print(f"Conexión reiniciada o abortada: {e}")
                running = False
            except Exception as e:
                print(f"Error durante la comunicación de datos: {e}")
                running = False
                
    except Exception as e:
        print(f"Error al manejar conexión en puerto {port}: {e}")
    
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
        print(f"Conexión finalizada para dispositivo en puerto {port}")

# Función para enviar los datos en formato JSON al puerto 7005
def send_to_json_port(port, data):
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
                "data": decoded_data,
                "timestamp": time.time()
            }
            
            # Convertir a JSON y enviar
            json_socket.sendall(json.dumps(json_data).encode('utf-8'))
        except ConnectionRefusedError:
            # Silenciosamente fallar si el puerto JSON no está disponible
            pass
        
    except Exception as e:
        print(f"Error al enviar datos al puerto JSON: {e}")
    
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