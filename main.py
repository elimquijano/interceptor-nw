#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import socket
import struct

# Lista de puertos objetivo (modifícalos según tus necesidades)
TARGET_PORTS = [5055, 5023, 5001, 8082]

def parse_ethernet(frame):
    eth_header = frame[:14]
    eth = struct.unpack("!6s6sH", eth_header)
    protocol = socket.ntohs(eth[2])
    return protocol, 14

def parse_ip(data, offset):
    ip_header = data[offset:offset+20]
    iph = struct.unpack("!BBHHHBBH4s4s", ip_header)
    version_ihl = iph[0]
    ihl = version_ihl & 0xF
    ip_header_length = ihl * 4
    protocol = iph[6]
    src_ip = socket.inet_ntoa(iph[8])
    dst_ip = socket.inet_ntoa(iph[9])
    return protocol, ip_header_length, src_ip, dst_ip

def parse_tcp(data, offset):
    tcp_header = data[offset:offset+20]
    tcph = struct.unpack("!HHLLBBHHH", tcp_header)
    src_port = tcph[0]
    dst_port = tcph[1]
    doff_reserved = tcph[4]
    tcp_header_length = (doff_reserved >> 4) * 4
    return src_port, dst_port, tcp_header_length

def parse_udp(data, offset):
    udp_header = data[offset:offset+8]
    udph = struct.unpack("!HHHH", udp_header)
    src_port = udph[0]
    dst_port = udph[1]
    udp_length = 8
    return src_port, dst_port, udp_length

def main():
    try:
        # Crea una socket RAW para capturar todos los paquetes a nivel de enlace
        s = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.ntohs(3))
    except socket.error as e:
        print("Error creando socket: {}".format(e))
        return

    print("Iniciando captura de tráfico para puertos {}...".format(TARGET_PORTS))
    
    while True:
        packet, _ = s.recvfrom(65565)
        eth_protocol, eth_length = parse_ethernet(packet)
        if eth_protocol != 0x0800:
            continue  # No es un paquete IP

        ip_protocol, ip_header_length, src_ip, dst_ip = parse_ip(packet, eth_length)
        
        # Procesa tráfico TCP
        if ip_protocol == 6:
            tcp_offset = eth_length + ip_header_length
            if len(packet) < tcp_offset+20:
                continue
            src_port, dst_port, tcp_header_length = parse_tcp(packet, tcp_offset)
            if src_port in TARGET_PORTS or dst_port in TARGET_PORTS:
                header_size = eth_length + ip_header_length + tcp_header_length
                data = packet[header_size:]
                print("TCP: {}:{} -> {}:{}  Data: {}".format(src_ip, src_port, dst_ip, dst_port, data))
        
        # Procesa tráfico UDP
        elif ip_protocol == 17:
            udp_offset = eth_length + ip_header_length
            if len(packet) < udp_offset+8:
                continue
            src_port, dst_port, udp_length = parse_udp(packet, udp_offset)
            if src_port in TARGET_PORTS or dst_port in TARGET_PORTS:
                header_size = eth_length + ip_header_length + udp_length
                data = packet[header_size:]
                print("UDP: {}:{} -> {}:{}  Data: {}".format(src_ip, src_port, dst_ip, dst_port, data))

if __name__ == '__main__':
    main()