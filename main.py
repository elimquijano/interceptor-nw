import socket
import json
import threading
import select
import time
from datetime import datetime
from collections import defaultdict

# Configuración de puertos y servidor Traccar
DEVICE_PORTS = {
    6013: 5013,  # Sinotrack: 6013 -> Traccar 5013
    6001: 5001,  # Coban: 6001 -> Traccar 5001
    6027: 5027,  # Teltonika: 6027 -> Traccar 5027
}
JSON_PORT = 7005
TRACCAR_HOST = "localhost"

# Estructuras para conexiones UDP persistentes
udp_traccar_connections = {}
udp_client_addresses = {}
udp_lock = threading.Lock()


def create_traccar_socket(port):
    """Crea un socket TCP con opciones de keep-alive"""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
    sock.connect((TRACCAR_HOST, DEVICE_PORTS[port]))
    return sock


def handle_device_data(port, client_socket):
    """Maneja conexiones TCP de dispositivos"""
    traccar_socket = None
    retries = 3

    try:
        # Intentar conectar a Traccar con reintentos
        while retries > 0:
            try:
                traccar_socket = create_traccar_socket(port)
                break
            except socket.error as e:
                retries -= 1
                if retries == 0:
                    print(
                        f"[{datetime.now()}] Error: No se pudo conectar a Traccar después de 3 intentos: {e}"
                    )
                    return
                time.sleep(1)

        client_socket.setblocking(0)
        traccar_socket.setblocking(0)
        inputs = [client_socket, traccar_socket]

        while True:
            readable, _, exceptional = select.select(inputs, [], inputs, 1)
            if exceptional:
                print(f"[{datetime.now()}] Error en socket, cerrando conexión...")
                break

            for sock in readable:
                try:
                    data = sock.recv(1024)
                    if not data:
                        break

                    # Determinar dirección del flujo de datos
                    if sock == client_socket:
                        traccar_socket.sendall(data)
                        send_to_json_port(port, data)
                    else:
                        client_socket.sendall(data)

                except ConnectionResetError:
                    print(f"[{datetime.now()}] Conexión reseteada, reconectando...")
                    traccar_socket.close()
                    traccar_socket = create_traccar_socket(port)
                    traccar_socket.sendall(data)
                    inputs[1] = traccar_socket

    except Exception as e:
        print(f"[{datetime.now()}] Error en handle_device_data: {e}")
    finally:
        client_socket.close()
        if traccar_socket:
            traccar_socket.close()


def handle_udp_data(port, data, client_address, udp_server_socket):
    """Maneja datos UDP con conexiones persistentes a Traccar"""
    device_id = f"{port}_{client_address[0]}_{client_address[1]}"

    with udp_lock:
        if (
            device_id not in udp_traccar_connections
            or udp_traccar_connections[device_id] is None
        ):
            try:
                udp_traccar_connections[device_id] = create_traccar_socket(port)
                udp_client_addresses[device_id] = client_address
            except Exception as e:
                print(f"[{datetime.now()}] Error al crear conexión UDP-TCP: {e}")
                return

    try:
        udp_traccar_connections[device_id].sendall(data)
        send_to_json_port(port, data)
        threading.Thread(
            target=listen_for_traccar_response, args=(device_id, udp_server_socket)
        ).start()
    except Exception as e:
        print(f"[{datetime.now()}] Error al enviar datos UDP: {e}")
        with udp_lock:
            udp_traccar_connections[device_id].close()
            udp_traccar_connections[device_id] = None


def listen_for_traccar_response(device_id, udp_server_socket):
    """Escucha respuestas de Traccar para conexiones UDP"""
    with udp_lock:
        if device_id not in udp_traccar_connections:
            return
        traccar_socket = udp_traccar_connections[device_id]
        client_address = udp_client_addresses.get(device_id)

    if not client_address:
        return

    try:
        traccar_socket.setblocking(0)
        ready, _, _ = select.select([traccar_socket], [], [], 0.5)
        if ready:
            response = traccar_socket.recv(1024)
            udp_server_socket.sendto(response, client_address)
    except Exception as e:
        print(f"[{datetime.now()}] Error en respuesta UDP: {e}")
        with udp_lock:
            traccar_socket.close()
            udp_traccar_connections[device_id] = None


def maintain_udp_connections():
    """Mantiene conexiones UDP persistentes activas"""
    while True:
        with udp_lock:
            for key in list(udp_traccar_connections.keys()):
                conn = udp_traccar_connections.get(key)
                if conn is None:
                    port = int(key.split("_")[0])
                    try:
                        udp_traccar_connections[key] = create_traccar_socket(port)
                    except:
                        pass
                else:
                    try:
                        conn.settimeout(0.1)
                        conn.sendall(b"")  # Heartbeat
                    except:
                        conn.close()
                        udp_traccar_connections[key] = None
        time.sleep(10)


def send_to_json_port(port, data):
    """Envía datos al puerto JSON"""
    try:
        json_data = {
            "timestamp": time.time(),
            "port": port,
            "data": data.hex() if isinstance(data, bytes) else data,
        }
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            sock.connect(("localhost", JSON_PORT))
            sock.sendall(json.dumps(json_data).encode())
    except:
        pass  # Silenciar errores del puerto JSON


def listen_for_data():
    """Función principal para escuchar conexiones"""
    tcp_sockets = {}
    udp_sockets = {}

    # Crear sockets TCP y UDP
    for port in DEVICE_PORTS:
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_sock.bind(("0.0.0.0", port))
        tcp_sock.listen(100)
        tcp_sockets[port] = tcp_sock

        udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        udp_sock.bind(("0.0.0.0", port))
        udp_sockets[port] = udp_sock

    # Iniciar hilo de mantenimiento UDP
    threading.Thread(target=maintain_udp_connections, daemon=True).start()

    try:
        while True:
            readable, _, _ = select.select(
                list(tcp_sockets.values()) + list(udp_sockets.values()), [], [], 0.1
            )

            for sock in readable:
                # Manejar conexiones TCP
                if isinstance(sock, socket.socket) and sock.type == socket.SOCK_STREAM:
                    port = next(p for p, s in tcp_sockets.items() if s == sock)
                    client, addr = sock.accept()
                    threading.Thread(
                        target=handle_device_data, args=(port, client), daemon=True
                    ).start()

                # Manejar datos UDP
                elif isinstance(sock, socket.socket) and sock.type == socket.SOCK_DGRAM:
                    port = next(p for p, s in udp_sockets.items() if s == sock)
                    data, addr = sock.recvfrom(1024)
                    threading.Thread(
                        target=handle_udp_data,
                        args=(port, data, addr, sock),
                        daemon=True,
                    ).start()

    except KeyboardInterrupt:
        print("Cerrando servidor...")


if __name__ == "__main__":
    listen_for_data()
