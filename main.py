import socket
import datetime
import struct

def start_capture():
    # Linux/Unix only version
    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
    print("Monitoreando puerto 5001...")
    
    try:
        while True:
            raw_packet = sock.recvfrom(65565)
            packet = raw_packet[0]
            addr = raw_packet[1]
            
            # Extract IP header
            ip_header = struct.unpack('!BBHHHBBH4s4s', packet[:20])
            iph_length = (ip_header[0] & 0xF) * 4
            
            # Get source IP
            source_ip = socket.inet_ntoa(ip_header[8])
            
            # Extract TCP header
            tcp_header = struct.unpack('!HHLLBBHHH', packet[iph_length:iph_length+20])
            source_port = tcp_header[0]
            dest_port = tcp_header[1]
            
            # Get data portion
            data_offset = iph_length + (tcp_header[4] >> 4) * 4
            data = packet[data_offset:]
            
            print("Paquete recibido - Puerto origen: %d, Puerto destino: %d" % (source_port, dest_port))
            if dest_port == 5001:
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print("[%s] Datos desde %s:%d" % (timestamp, source_ip, source_port))
                if data:
                    print("Payload: %s" % data.encode('hex'))
                print("-" * 50)
                
    except KeyboardInterrupt:
        print("\nCaptura terminada")
        sock.close()

if __name__ == "__main__":
    start_capture()