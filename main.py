import socket
import json
import threading

# Puertos de escucha para los dispositivos GPS
DEVICE_PORTS = {
    6013: 5013,  # Sinotrack: 6013 -> Traccar 5013
    6001: 5001,  # Coban: 6001 -> Traccar 5001
    6027: 5027,  # Teltonika: 6027 -> Traccar 5027
}

# Puerto adicional para enviar la data en formato JSON
JSON_PORT = 7005

# Dirección del servidor Traccar para cada tipo de dispositivo
TRACCAR_HOST = 'localhost'

# Función para manejar la conexión y escuchar los datos
def listen_for_data():
    # Crear el socket TCP para escuchar en múltiples puertos
    server_sockets = {}
    
    for port in DEVICE_PORTS.keys():
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('0.0.0.0', port))
        server_socket.listen(25)
        server_sockets[port] = server_socket
        print(f"Escuchando en el puerto {port}...")

    # Mantener el servidor funcionando
    while True:
        for port, server_socket in server_sockets.items():
            client_socket, client_address = server_socket.accept()
            print(f"Conexión aceptada desde {client_address} en el puerto {port}")

            # Crear un hilo para manejar cada conexión
            threading.Thread(target=handle_device_data, args=(port, client_socket)).start()

# Función para manejar los datos de un dispositivo
def handle_device_data(port, client_socket):
    try:
        # Conectar al servidor Traccar para el puerto correspondiente
        traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        traccar_socket.connect((TRACCAR_HOST, DEVICE_PORTS[port]))

        # Recibir datos del dispositivo
        while True:
            data = client_socket.recv(1024)  # Tamaño máximo de paquete: 1024 bytes
            if not data:
                break  # Si no se recibe más data, cerrar la conexión

            # Imprimir los datos recibidos
            print(f"Datos recibidos del dispositivo en puerto {port}: {data}")

            # Enviar los datos a Traccar
            traccar_socket.sendall(data)
            print(f"Datos reenviados a Traccar en puerto {DEVICE_PORTS[port]}: {data}")

            # Enviar los datos al puerto 7005 en formato JSON
            send_to_json_port(port, data)

    except Exception as e:
        print(f"Error al recibir o reenviar datos: {e}")
    finally:
        client_socket.close()
        print(f"Conexión cerrada con el dispositivo en el puerto {port}")

# Función para enviar los datos en formato JSON al puerto 7005
def send_to_json_port(port, data):
    try:
        # Crear un socket para enviar los datos en formato JSON
        json_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Conectar al puerto 7005
        json_socket.connect(('localhost', JSON_PORT))

        # Crear el diccionario con los datos y puerto
        json_data = {
            "port": port,
            "data": data.decode('utf-8')  # Asumimos que los datos son cadenas UTF-8
        }

        # Convertir a JSON y enviar
        json_socket.sendall(json.dumps(json_data).encode('utf-8'))
        print(f"Datos enviados al puerto 7005: {json_data}")

        # Cerrar el socket
        json_socket.close()

    except Exception as e:
        print(f"Error al enviar datos al puerto 7005: {e}")

# Iniciar el servidor
if __name__ == "__main__":
    listen_for_data()
