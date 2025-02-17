import socket
import datetime
import struct
import binascii

def monitor_packet(data, source_ip, dest_port):
    # Check GPS protocols
    try:
        if b'$$' in data:  # Coban
            print("[%s] Coban data desde %s" % (
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                source_ip
            ))
        elif b'*HQ' in data:  # GT06
            print("[%s] GT06 data desde %s" % (
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                source_ip
            ))
        print("Puerto: %d" % dest_port)
        print("Data: %s" % data)
        print("-" * 50)
    except:
        pass

def start_capture():
    # Create raw socket (no port binding)
    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
    print("Monitoreando trafico GPS...")

    try:
        while True:
            packet = sock.recvfrom(65565)
            ip_header = packet[0][0:20]
            iph = struct.unpack('!BBHHHBBH4s4s', ip_header)
            
            # Get IP and ports
            source_ip = socket.inet_ntoa(iph[8])
            tcp_header = packet[0][20:40]
            tcph = struct.unpack('!HHLLBBHHH', tcp_header)
            dest_port = tcph[1]
            
            # Get data portion
            header_size = 20 + (tcph[4] >> 4) * 4
            data = packet[0][header_size:]
            
            # Monitor specific ports
            if dest_port in [5001, 5003, 5004, 5005] and len(data) > 0:
                monitor_packet(data, source_ip, dest_port)
                
    except KeyboardInterrupt:
        print("\nCaptura terminada")
        sock.close()

if __name__ == "__main__":
    start_capture()