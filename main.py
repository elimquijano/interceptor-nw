#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import socket
import struct

def main():
    try:
        # Crea una socket RAW para capturar todo el tráfico
        s = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(3))
    except socket.error as msg:
        print("Error al crear la socket: {} {}".format(msg[0], msg[1]))
        return

    print("Capturando paquetes en tiempo real en el puerto 8082...")
    
    while True:
        packet, addr = s.recvfrom(65565)
        eth_length = 14
        eth_header = packet[:eth_length]
        eth = struct.unpack("!6s6sH", eth_header)
        eth_protocol = socket.ntohs(eth[2])
        
        # Procesa solo paquetes IP (0x0800)
        if eth_protocol == 0x0800:
            # Extrae el header IP
            ip_header = packet[eth_length:eth_length+20]
            iph = struct.unpack("!BBHHHBBH4s4s", ip_header)
            version_ihl = iph[0]
            ihl = version_ihl & 0xF
            iph_length = ihl * 4
            protocol = iph[6]
            
            # Procesa solo paquetes TCP (protocolo 6)
            if protocol == 6:
                t = eth_length + iph_length
                tcp_header = packet[t:t+20]
                tcph = struct.unpack("!HHLLBBHHH", tcp_header)
                src_port = tcph[0]
                dest_port = tcph[1]
                
                # Filtra paquetes donde el puerto origen o destino es 8082
                if src_port == 8082 or dest_port == 8082:
                    doff_reserved = tcph[4]
                    tcph_length = (doff_reserved >> 4) * 4
                    header_size = eth_length + iph_length + tcph_length
                    data = packet[header_size:]
                    
                    print("Paquete: {} -> {}  Payload: {}".format(src_port, dest_port, data))

if __name__ == '__main__':
    main()