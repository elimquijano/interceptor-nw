import socket
import threading
import json
from datetime import datetime

DEVICE_PORTS = [6001, 6013]  # Ports GPS devices connect to
TRACCAR_PORTS = [5001, 5013]  # Ports Traccar will listen on
ADDITIONAL_PORT = 7005  # Additional port to send data to

def handle_client(client_socket, client_address, device_port, traccar_port):
    print(f"Proxy: Connection from {client_address} on device port {device_port}")
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break

            print(f"Proxy: Received data from client on device port {device_port}: {data.decode('utf-8')}")

            # Send to Traccar
            try:
                traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                traccar_socket.connect(('127.0.0.1', traccar_port))
                traccar_socket.sendall(data)
                traccar_socket.close()
                print(f"Proxy: Sent data to Traccar on port {traccar_port}")
            except Exception as e:
                print(f"Proxy: Error sending to Traccar on port {traccar_port}: {e}")

            # Send to additional port
            try:
                additional_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                additional_socket.connect(('127.0.0.1', ADDITIONAL_PORT))
                # Include the device port in the data being sent
                data_dict = {"port": device_port, "data": data.decode('utf-8')}
                data_json = json.dumps(data_dict).encode('utf-8')
                additional_socket.sendall(data_json)
                additional_socket.close()
                print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                print(f"Proxy: Sent data to additional port {ADDITIONAL_PORT} from device port {device_port}")
            except Exception as e:
                print(f"Proxy: Error sending to additional port {ADDITIONAL_PORT}: {e}")

    except Exception as e:
        print(f"Proxy: Error handling client: {e}")
    finally:
        client_socket.close()
        print(f"Proxy: Connection closed with {client_address}")

def start_proxy_server(device_port, traccar_port):
    proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        proxy_socket.bind(('0.0.0.0', device_port))
    except OSError as e:
        print(f"Error binding to device port {device_port}: {e}")
        return
    proxy_socket.listen(5)
    print(f"Proxy: Listening on device port {device_port}")

    try:
        while True:
            client_socket, client_address = proxy_socket.accept()
            thread = threading.Thread(target=handle_client, args=(client_socket, client_address, device_port, traccar_port))
            thread.start()
    except KeyboardInterrupt:
        print("Proxy: Shutting down.")
    finally:
        proxy_socket.close()

if __name__ == "__main__":
    if not (len(DEVICE_PORTS) == len(TRACCAR_PORTS)):
        print("Error: DEVICE_PORTS, and TRACCAR_PORTS must have the same number of elements.")
    else:
        for i in range(len(DEVICE_PORTS)):
            threading.Thread(target=start_proxy_server, args=(DEVICE_PORTS[i], TRACCAR_PORTS[i])).start()