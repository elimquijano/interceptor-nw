import socket
import datetime

def start_capture():
    # Create raw socket to just monitor port 5001
    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
    print("Monitoreando puerto 5001...")
    
    try:
        while True:
            # Receive packet
            raw_packet = sock.recvfrom(65565)
            packet = raw_packet[0]
            
            # Extract TCP header and data
            ip_header = packet[0:20]
            tcp_header = packet[20:40]
            
            # Check if packet is for port 5001
            dest_port = (tcp_header[2] << 8) + tcp_header[3]
            if dest_port == 5001:
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print("[%s] Datos capturados:" % timestamp)
                print("Raw data: %s" % packet.encode('hex'))
                print("-" * 50)
                
    except KeyboardInterrupt:
        print("\nCaptura terminada")
        sock.close()

if __name__ == "__main__":
    start_capture()