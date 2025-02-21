import socket
import threading

def handle_client(conn, addr):
    print("Conexión establecida por {}".format(addr))
    while True:
        data = conn.recv(1024)
        if not data:
            break
        print("Datos recibidos de {}: {}".format(addr, data.decode('utf-8')))
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
    port = 5013
    start_tcp_server(port)