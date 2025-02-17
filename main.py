import socket
import datetime
import binascii

def start_capture():
    # Create TCP server
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', 5001))
    server.listen(5)
    print("Escuchando en puerto 5001...")
    
    try:
        while True:
            client, addr = server.accept()
            print("[%s] Conexión desde %s:%d" % (
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                addr[0], 
                addr[1]
            ))
            
            data = client.recv(1024)
            if data:
                print("Data (hex): %s" % binascii.hexlify(data))
                print("Data (ascii): %s" % data)
                print("-" * 50)
            
            client.close()
            
    except KeyboardInterrupt:
        print("\nCaptura terminada")
        server.close()

if __name__ == "__main__":
    start_capture()