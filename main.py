from scapy.all import *
import datetime

def packet_callback(packet):
    if packet.haslayer(TCP) and packet.haslayer(Raw):
        if packet[TCP].dport == 5001:
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            src_ip = packet[IP].src
            payload = packet[Raw].load
            
            print("[%s] Datos desde %s:" % (timestamp, src_ip))
            print("Payload (hex): %s" % payload.encode('hex'))
            try:
                print("Payload (utf-8, si es legible): %s" % payload)
            except UnicodeDecodeError:
                print("Payload no es texto legible")
            print("-" * 50)

def start_capture():
    print("Iniciando captura de paquetes en puerto 5001...")
    sniff(filter="tcp and port 5001", prn=packet_callback, store=0)

if __name__ == "__main__":
    start_capture()