import socket
import datetime
import binascii
import struct

def parse_gps_data(data):
    # Parse diferentes formatos GPS
    if data.startswith("$$"):  # Coban format
        parts = data.split(",")
        if len(parts) >= 12:
            try:
                imei = parts[0][2:]  # Remove $$
                command = parts[1]
                lat = float(parts[4][0:2]) + float(parts[4][2:])/60
                lon = float(parts[6][0:3]) + float(parts[6][3:])/60
                speed = float(parts[7])
                direction = float(parts[8])
                date = parts[2]
                return {
                    'tipo': 'Coban',
                    'imei': imei,
                    'lat': lat,
                    'lon': lon,
                    'velocidad': speed,
                    'direccion': direction,
                    'fecha': date
                }
            except:
                pass
                
    elif data.startswith("*HQ"):  # GT06 format
        parts = data.split(",")
        if len(parts) >= 10:
            try:
                imei = parts[1]
                lat = float(parts[5])
                lon = float(parts[7])
                speed = float(parts[9])
                return {
                    'tipo': 'GT06',
                    'imei': imei,
                    'lat': lat,
                    'lon': lon,
                    'velocidad': speed
                }
            except:
                pass
    
    return None
def start_capture():
    sock = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_TCP)
    print("Monitoreando puertos GPS comunes...")
    
    # Lista de puertos comunes para GPS
    gps_ports = [5001, 5003, 5004, 5005, 5008, 5010, 7001, 8001, 9001]
    
    try:
        while True:
            packet = sock.recvfrom(65565)
            raw_data = packet[0]
            
            ip_header = raw_data[0:20]
            iph = struct.unpack('!BBHHHBBH4s4s', ip_header)
            source_ip = socket.inet_ntoa(iph[8])
            
            tcp_header = raw_data[20:40]
            tcph = struct.unpack('!HHLLBBHHH', tcp_header)
            source_port = tcph[0]
            dest_port = tcph[1]
            
            header_size = 20 + (tcph[4] >> 4) * 4
            data = raw_data[header_size:]
            
            if dest_port in gps_ports and len(data) > 0:
                print("Puerto detectado: %d" % dest_port)
                gps_data = parse_gps_data(data)
                if gps_data:
                    print("[%s] GPS Data:" % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    for key, value in gps_data.items():
                        print("%s: %s" % (key, value))
                    print("-" * 50)

    except KeyboardInterrupt:
        print("\nCaptura terminada")
        sock.close()


if __name__ == "__main__":
    start_capture()