import socket
import datetime
import struct

def start_capture():
    # Create raw socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
    print("Monitoreando puerto 5001...")
    
    try:
        while True:
            # Receive packet
            raw_packet = sock.recvfrom(65565)
            packet = raw_packet[0]
            
            # Get IP header
            ip_header = struct.unpack('!BBHHHBBH4s4s', packet[:20])
            iph_length = (ip_header[0] & 0xF) * 4
            
            # Get TCP header
            tcp_header = struct.unpack('!HHLLBBHHH', packet[iph_length:iph_length+20])
            dest_port = tcp_header[1]
            
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