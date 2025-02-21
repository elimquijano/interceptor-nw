import socket
import time

# Puerto de escucha para el dispositivo GPS
DEVICE_PORT = 6013
# Puerto donde Traccar está escuchando para dispositivos Sinotrack
TRACCAR_HOST = 'localhost'  # Dirección del servidor Traccar
TRACCAR_PORT = 5013  # Puerto de Traccar para Sinotrack (puerto 5013)

# Tiempo para enviar un paquete "keep-alive" (por ejemplo, cada 30 segundos)
KEEP_ALIVE_INTERVAL = 30  # Segundos

# Función para manejar la conexión y escuchar los datos
def listen_for_data():
    # Crear el socket TCP
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        # Vincular el socket al puerto
        server_socket.bind(('0.0.0.0', DEVICE_PORT))
        server_socket.listen(5)  # Escuchar hasta 5 conexiones simultáneas

        print(f"Escuchando en el puerto {DEVICE_PORT}...")

        while True:
            # Aceptar una conexión entrante
            client_socket, client_address = server_socket.accept()
            print(f"Conexión aceptada desde {client_address}")

            try:
                # Recibir los datos
                while True:
                    data = client_socket.recv(1024)  # Tamaño máximo de paquete: 1024 bytes
                    if not data:
                        break  # Si no se recibe más data, cerrar la conexión

                    # Imprimir los datos recibidos
                    print(f"Datos recibidos: {data}")

                    # Enviar los datos a Traccar de forma transparente
                    forward_to_traccar(data)

                    # Aquí puedes realizar el tratamiento de los datos para tus necesidades
                    # process_data(data)

                    # Enviar keep-alive después de recibir cada paquete
                    send_keep_alive()

            except Exception as e:
                print(f"Error al recibir datos: {e}")
            finally:
                client_socket.close()
                print(f"Conexión cerrada con {client_address}")
    except Exception as e:
        print(f"Error al iniciar el servidor: {e}")
    finally:
        server_socket.close()

# Función para reenviar los datos a Traccar
def forward_to_traccar(data):
    try:
        # Crear un socket para la conexión con Traccar
        traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Conectar al servidor Traccar
        traccar_socket.connect((TRACCAR_HOST, TRACCAR_PORT))

        # Enviar los datos a Traccar
        traccar_socket.sendall(data)

        # Cerrar la conexión con Traccar
        traccar_socket.close()

        print(f"Datos reenviados a Traccar: {data}")
    
    except Exception as e:
        print(f"Error al reenviar los datos a Traccar: {e}")

# Función para enviar un paquete "keep-alive" a Traccar
def send_keep_alive():
    try:
        # Crear un socket para la conexión con Traccar
        traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Conectar al servidor Traccar
        traccar_socket.connect((TRACCAR_HOST, TRACCAR_PORT))

        # Enviar un paquete vacío o mínimo para mantener viva la conexión
        traccar_socket.sendall(b'\x00')  # Paquete vacío (simulando un keep-alive)

        # Cerrar la conexión con Traccar
        traccar_socket.close()

        print("Keep-alive enviado a Traccar")

    except Exception as e:
        print(f"Error al enviar keep-alive a Traccar: {e}")

# Iniciar la función para escuchar
if __name__ == "__main__":
    listen_for_data()
