import socket
import threading

def handle_client(conn, addr):
    print(f"Conexión establecida por {addr}")
    while True:
        data = conn.recv(1024)  # Recibir datos
        if not data:
            break  # Salir si no hay más datos
        print(f"Datos recibidos de {addr}: {data.decode('utf-8')}")
        # Aquí puedes procesar los datos como desees
    conn.close()

def start_tcp_server(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('0.0.0.0', port))
        s.listen()
        print(f"Servidor TCP escuchando en el puerto {port}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn, addr)).start()

# Iniciar servidores TCP en los puertos 5055 y 5056
threading.Thread(target=start_tcp_server, args=(5001,)).start()
#threading.Thread(target=start_tcp_server, args=(5056,)).start()
