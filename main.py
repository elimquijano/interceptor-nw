import socket
import threading
import json
from datetime import datetime

DEVICE_PORTS = [6001, 6013]  # Puertos de los dispositivos GPS
TRACCAR_PORTS = [5001, 5013]  # Puertos en los que Traccar escucha
ADDITIONAL_PORT = 7005  # Puerto adicional para enviar los datos duplicados

# Función para manejar los datos de los clientes
def handle_client(client_socket, client_address, device_port, traccar_port):
    print(f"Proxy: Conexión desde {client_address} en el puerto del dispositivo {device_port}")
    try:
        while True:
            # Recibir los datos del dispositivo
            data = client_socket.recv(1024)
            if not data:
                break  # Si no hay más datos, cerrar la conexión

            print(f"Proxy: Recibido datos del cliente en el puerto {device_port}: {data.decode('utf-8')}")

            # Enviar los datos a Traccar
            try:
                traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # Usar UDP
                traccar_socket.sendto(data, ('127.0.0.1', traccar_port))
                traccar_socket.close()
                print(f"Proxy: Datos enviados a Traccar en el puerto {traccar_port}")
            except Exception as e:
                print(f"Proxy: Error al enviar a Traccar en el puerto {traccar_port}: {e}")

            # Enviar los datos a un puerto adicional para tu propio tratamiento
            try:
                additional_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # Usar UDP
                # Incluir el puerto del dispositivo en los datos enviados
                data_dict = {"port": device_port, "data": data.decode('utf-8')}
                data_json = json.dumps(data_dict).encode('utf-8')
                additional_socket.sendto(data_json, ('127.0.0.1', ADDITIONAL_PORT))
                additional_socket.close()
                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                print(f"Proxy: Datos enviados al puerto adicional {ADDITIONAL_PORT} desde el puerto del dispositivo {device_port}")
            except Exception as e:
                print(f"Proxy: Error al enviar al puerto adicional {ADDITIONAL_PORT}: {e}")

    except Exception as e:
        print(f"Proxy: Error al manejar el cliente: {e}")
    finally:
        client_socket.close()
        print(f"Proxy: Conexión cerrada con {client_address}")

# Función para iniciar el servidor proxy
def start_proxy_server(device_port, traccar_port):
    proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # Usar UDP
    try:
        proxy_socket.bind(('0.0.0.0', device_port))
    except OSError as e:
        print(f"Error al vincular el puerto del dispositivo {device_port}: {e}")
        return
    print(f"Proxy: Escuchando en el puerto del dispositivo {device_port}")

    try:
        while True:
            # Esperar a recibir datos del dispositivo GPS
            data, client_address = proxy_socket.recvfrom(1024)
            print(f"Proxy: Recibido datos de {client_address} en el puerto {device_port}: {data.decode('utf-8')}")
            # Enviar los datos a Traccar y al puerto adicional
            traccar_port = TRACCAR_PORTS[DEVICE_PORTS.index(device_port)]  # Obtener el puerto de Traccar correspondiente
            handle_client(proxy_socket, client_address, device_port, traccar_port)
    except KeyboardInterrupt:
        print("Proxy: Cerrando servidor.")
    finally:
        proxy_socket.close()

# Función principal
if __name__ == "__main__":
    if not (len(DEVICE_PORTS) == len(TRACCAR_PORTS)):
        print("Error: Los puertos de los dispositivos y Traccar deben tener el mismo número de elementos.")
    else:
        # Iniciar el servidor proxy para cada dispositivo GPS y su puerto correspondiente en Traccar
        for i in range(len(DEVICE_PORTS)):
            threading.Thread(target=start_proxy_server, args=(DEVICE_PORTS[i], TRACCAR_PORTS[i])).start()
