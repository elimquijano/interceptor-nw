import socket

# Puerto de escucha para el dispositivo GPS
DEVICE_PORT = 6013
# Puerto donde Traccar está escuchando para dispositivos Sinotrack
TRACCAR_HOST = 'localhost'  # Dirección del servidor Traccar
TRACCAR_PORT = 5013  # Puerto de Traccar para Sinotrack (puerto 5013)

# Crear un socket global para Traccar (mantener la conexión abierta)
traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Función para manejar la conexión y escuchar los datos
def listen_for_data():
    # Crear el socket TCP para escuchar dispositivos
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        # Vincular el socket al puerto
        server_socket.bind(('0.0.0.0', DEVICE_PORT))
        server_socket.listen(5)  # Escuchar hasta 5 conexiones simultáneas

        print(f"Escuchando en el puerto {DEVICE_PORT}...")

        # Establecer la conexión persistente con Traccar al inicio
        traccar_socket.connect((TRACCAR_HOST, TRACCAR_PORT))
        print(f"Conexión persistente a Traccar establecida en {TRACCAR_HOST}:{TRACCAR_PORT}")

        while True:
            # Aceptar una conexión entrante desde un dispositivo GPS
            client_socket, client_address = server_socket.accept()
            print(f"Conexión aceptada desde {client_address}")

            try:
                # Recibir los datos del dispositivo GPS
                while True:
                    data = client_socket.recv(1024)  # Tamaño máximo de paquete: 1024 bytes
                    if not data:
                        break  # Si no se recibe más data, cerrar la conexión

                    # Imprimir los datos recibidos
                    print(f"Datos recibidos: {data}")

                    # Enviar los datos a Traccar (manteniendo la conexión persistente)
                    traccar_socket.sendall(data)
                    print(f"Datos reenviados a Traccar: {data}")

            except Exception as e:
                print(f"Error al recibir datos: {e}")
            finally:
                client_socket.close()
                print(f"Conexión cerrada con {client_address}")
    except Exception as e:
        print(f"Error al iniciar el servidor: {e}")
    finally:
        server_socket.close()

# Iniciar la función para escuchar
if __name__ == "__main__":
    listen_for_data()
