import socket
import datetime
import struct
import binascii

def parse_gps_data(data):
    try:
        # Convert bytes to string
        data_str = data.strip()
        
        # Check for common GPS formats
        if "$GPRMC" in data_str:
            return "NMEA Format: " + data_str
        elif "##" in data_str:  # GT06/TK103
            return "GT06/TK103 Format: " + data_str
        elif "$POS" in data_str:  # Generic position
            return "Position Format: " + data_str
        else:
            return "Raw HEX: " + binascii.hexlify(data)
    except:
        return "Raw HEX: " + binascii.hexlify(data)

def start_capture():
    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
    print("Monitoreando puertos GPS (5001, 8082)...")
    
    try:
        while True:
            raw_packet = sock.recvfrom(65565)
            packet = raw_packet[0]
            addr = raw_packet[1]
            
            ip_header = struct.unpack('!BBHHHBBH4s4s', packet[:20])
            iph_length = (ip_header[0] & 0xF) * 4
            source_ip = socket.inet_ntoa(ip_header[8])
            
            tcp_header = struct.unpack('!HHLLBBHHH', packet[iph_length:iph_length+20])
            source_port = tcp_header[0]
            dest_port = tcp_header[1]
            
            data_offset = iph_length + (tcp_header[4] >> 4) * 4
            data = packet[data_offset:]
            
            if dest_port in [5001, 8082] and len(data) > 0:
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                print("\n[%s] Datos desde %s:%d" % (timestamp, source_ip, source_port))
                print("Puerto destino: %d" % dest_port)
                print(parse_gps_data(data))
                print("-" * 50)
                
    except KeyboardInterrupt:
        print("\nCaptura terminada")
        sock.close()

if __name__ == "__main__":
    start_capture()