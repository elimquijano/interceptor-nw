import socket
import threading
import json

def handle_client(conn, addr):
    #print("Conexión establecida por {}".format(addr))
    while True:
        data = conn.recv(1024)
        if not data:
            break
        try:
            received_json = json.loads(data.decode('utf-8'))
            port = received_json["port"]
            message_data = received_json["data"]
            if port == 5013:
                print(f"SINOTRACK: {message_data}") 
        except json.JSONDecodeError as e:
            print(f"Receiver: Invalid JSON received: {e}")
            print(f"Receiver: Raw data received: {data.decode('utf-8')}") # Print raw data for debugging
        except KeyError as e:
                    print(f"Receiver: Missing key in JSON: {e}") 
    conn.close()

def start_tcp_server(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('0.0.0.0', port))
    s.listen(5)
    print("Servidor TCP escuchando en el puerto {}".format(port))
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()

if __name__ == "__main__":
    port = 7012
    start_tcp_server(port)