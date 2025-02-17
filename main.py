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
    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
    print("Monitoreando trafico TCP puerto 8082...")

    try:
        while True:
            packet = sock.recvfrom(65565)
            ip_header = packet[0][0:20]
            iph = struct.unpack('!BBHHHBBH4s4s', ip_header)
            
            source_ip = socket.inet_ntoa(iph[8])
            tcp_header = packet[0][20:40]
            tcph = struct.unpack('!HHLLBBHHH', tcp_header)
            source_port = tcph[0]
            dest_port = tcph[1]
            
            # Get data portion
            header_size = 20 + (tcph[4] >> 4) * 4
            data = packet[0][header_size:]
            
            # Monitor port 8082
            if dest_port == 8082 and len(data) > 0:
                print("\nDatos hacia puerto 8082:")
                print("Origen: %s:%d" % (source_ip, source_port))
                print("Datos HEX: %s" % binascii.hexlify(data))
                try:
                    print("Datos ASCII: %s" % data)
                except:
                    pass
                print("-" * 50)
                
    except KeyboardInterrupt:
        print("\nCaptura terminada")
        sock.close()

if __name__ == "__main__":
    start_capture()