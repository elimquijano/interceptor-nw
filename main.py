# -*- coding: utf-8 -*-
import socket
import threading

def handle_client(conn, addr):
    print("Conexión establecida por {}".format(addr))
    while True:
        data = conn.recv(1024)  # Recibir datos
        if not data:
            break  # Salir si no hay más datos
        print("Datos recibidos de {}: {}".format(addr, data.decode('utf-8')))
        # Aquí puedes procesar los datos como desees
    conn.close()

def start_tcp_server(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('0.0.0.0', port))
    s.listen(5)
    print("Servidor TCP escuchando en el puerto {}".format(port))
    while True:
        conn, addr = s.accept()
        threading.Thread(target=handle_client, args=(conn, addr)).start()

# Iniciar servidor TCP en el puerto 7012
threading.Thread(target=start_tcp_server, args=(7012,)).start()
