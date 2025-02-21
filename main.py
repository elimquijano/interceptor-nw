import socket

# Puerto de escucha para el dispositivo GPS
DEVICE_PORT = 6013

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
                    print(f"Datos recibidos: {data.decode('utf-8')}")
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
