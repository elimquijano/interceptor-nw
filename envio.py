import socket
from datetime import datetime

# Configura la dirección IP y el puerto del servidor
ip_servidor = '127.0.0.1'  # Reemplaza con la IP del servidor
puerto = 6013                 # Reemplaza con el puerto del servidor

# Crea un socket TCP
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

try:
    # Conéctate al servidor
    sock.connect((ip_servidor, puerto))
    
    # Datos simulando un dispositivo GPS
    datos_gps = "*HQ,9176022452,V1,163619,A,0956.1198,S,07614.8670,W,000.00,058,200255,FFF7FBFF,716,10,6510,29263"  # Latitud y longitud
    sock.sendall(datos_gps.encode('utf-8'))  # Envía los datos
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("Datos enviados")

finally:
    sock.close()  # Cierra el socket
