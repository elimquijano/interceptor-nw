import socket
import json
import threading
import select
import time
from datetime import datetime
from queue import Queue
import logging
import os

# Configure logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, f"proxy_{datetime.now().strftime('%Y%m%d')}.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("gps_proxy")

# Ports for GPS devices
DEVICE_PORTS = {
    6013: 5013,  # Sinotrack: 6013 -> Traccar 5013
    6001: 5001,  # Coban: 6001 -> Traccar 5001
    6027: 5027,  # Teltonika: 6027 -> Traccar 5027
}

# Additional port for JSON data
JSON_PORT = 7005

# Traccar server address
TRACCAR_HOST = "localhost"

# Use a thread pool for handling connections
MAX_WORKERS = 300  # Maximum number of worker threads
worker_queue = Queue()
active_workers = 0
worker_lock = threading.Lock()

# Dictionary to maintain TCP connections to Traccar
tcp_connections = {}
tcp_lock = threading.Lock()

# Dictionary to maintain UDP connections
udp_connections = {}
udp_client_addresses = {}
udp_lock = threading.Lock()

# Queue for JSON data
json_queue = Queue(maxsize=5000)  # Limit queue size to prevent memory issues

# Function to initialize worker threads
def init_workers():
    for _ in range(MAX_WORKERS):
        worker = threading.Thread(target=worker_function)
        worker.daemon = True
        worker.start()

# Worker function to process tasks from queue
def worker_function():
    global active_workers
    while True:
        try:
            task, args = worker_queue.get()
            with worker_lock:
                active_workers += 1
            
            try:
                task(*args)
            except Exception as e:
                logger.error(f"Error in worker task: {e}")
            
            with worker_lock:
                active_workers -= 1
            
            worker_queue.task_done()
        except Exception as e:
            logger.error(f"Worker error: {e}")

# Function to handle listening for data on multiple ports
def listen_for_data():
    # Create TCP and UDP server sockets
    tcp_server_sockets = {}
    udp_server_sockets = {}

    for port in DEVICE_PORTS.keys():
        # Configure TCP socket
        try:
            tcp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            tcp_server_socket.setblocking(0)
            tcp_server_socket.bind(("0.0.0.0", port))
            tcp_server_socket.listen(200)  # Allow up to 200 pending connections
            tcp_server_sockets[port] = tcp_server_socket
            logger.info(f"Listening for TCP on port {port}...")
        except Exception as e:
            logger.error(f"Failed to set up TCP socket on port {port}: {e}")

        # Configure UDP socket
        try:
            udp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            udp_server_socket.setblocking(0)
            udp_server_socket.bind(("0.0.0.0", port))
            udp_server_sockets[port] = udp_server_socket
            logger.info(f"Listening for UDP on port {port}...")
        except Exception as e:
            logger.error(f"Failed to set up UDP socket on port {port}: {e}")

    # All server sockets for select
    inputs = list(tcp_server_sockets.values()) + list(udp_server_sockets.values())
    
    # Start JSON sender thread
    json_sender = threading.Thread(target=process_json_queue)
    json_sender.daemon = True
    json_sender.start()
    
    # Start connection monitor thread
    connection_monitor = threading.Thread(target=monitor_connections)
    connection_monitor.daemon = True
    connection_monitor.start()

    # Keep server running
    try:
        while True:
            try:
                # Use select to monitor multiple sockets without blocking
                readable, _, exceptional = select.select(inputs, [], inputs, 0.01)

                for sock in readable:
                    # Handle incoming TCP connections
                    for port, server_sock in tcp_server_sockets.items():
                        if sock == server_sock:
                            client_socket, client_address = server_sock.accept()
                            client_socket.setblocking(0)
                            
                            # Queue the connection handling task
                            worker_queue.put((handle_tcp_client, (port, client_socket, client_address)))

                    # Handle incoming UDP data
                    for port, server_sock in udp_server_sockets.items():
                        if sock == server_sock:
                            try:
                                data, client_address = server_sock.recvfrom(4096)
                                
                                # Queue the UDP data handling task
                                worker_queue.put((handle_udp_data, (port, data, client_address, server_sock)))
                            except Exception as e:
                                logger.error(f"Error receiving UDP data on port {port}: {e}")

                # Check problem sockets
                for sock in exceptional:
                    logger.error(f"Socket error, closing...")
                    sock.close()
                    inputs.remove(sock)

            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                # Small sleep to avoid CPU overload in case of repeated errors
                time.sleep(0.1)

    except KeyboardInterrupt:
        logger.info("Shutting down server...")
    finally:
        # Clean shutdown
        for _, socket in tcp_server_sockets.items():
            socket.close()
        for _, socket in udp_server_sockets.items():
            socket.close()
        
        # Close all connections
        with tcp_lock:
            for conn in tcp_connections.values():
                try:
                    if conn:
                        conn.close()
                except:
                    pass
        
        with udp_lock:
            for conn in udp_connections.values():
                try:
                    if conn:
                        conn.close()
                except:
                    pass

# Function to monitor connections
def monitor_connections():
    while True:
        try:
            # Log stats periodically
            with worker_lock:
                active = active_workers
            
            with tcp_lock:
                tcp_count = len(tcp_connections)
            
            with udp_lock:
                udp_count = len(udp_connections)
            
            json_size = json_queue.qsize()
            
            logger.info(f"Status: Active workers: {active}/{MAX_WORKERS}, TCP connections: {tcp_count}, UDP connections: {udp_count}, JSON queue: {json_size}")
            
            # Clean up stale connections
            clean_stale_connections()
            
        except Exception as e:
            logger.error(f"Error in connection monitor: {e}")
        
        time.sleep(60)  # Check every minute

# Clean up stale connections
def clean_stale_connections():
    current_time = time.time()
    cleanup_threshold = 3600  # 1 hour
    
    with tcp_lock:
        stale_connections = []
        for key, (conn, last_activity) in list(tcp_connections.items()):
            if current_time - last_activity > cleanup_threshold:
                stale_connections.append(key)
        
        for key in stale_connections:
            try:
                conn, _ = tcp_connections[key]
                conn.close()
            except:
                pass
            del tcp_connections[key]
    
    with udp_lock:
        stale_connections = []
        for key, (conn, last_activity) in list(udp_connections.items()):
            if current_time - last_activity > cleanup_threshold:
                stale_connections.append(key)
        
        for key in stale_connections:
            try:
                conn, _ = udp_connections[key]
                conn.close()
            except:
                pass
            del udp_connections[key]
            if key in udp_client_addresses:
                del udp_client_addresses[key]

# Function to handle TCP client
def handle_tcp_client(port, client_socket, client_address):
    traccar_socket = None
    client_id = f"TCP_{port}_{client_address[0]}_{client_address[1]}_{time.time()}"
    
    try:
        # Connect to Traccar server
        traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        traccar_socket.settimeout(5)
        traccar_socket.connect((TRACCAR_HOST, DEVICE_PORTS[port]))
        traccar_socket.setblocking(0)
        
        # Store connection with timestamp
        with tcp_lock:
            tcp_connections[client_id] = (traccar_socket, time.time())
        
        # Set up for bidirectional communication
        running = True
        
        while running:
            # Wait for data from either side
            readable, _, exceptional = select.select([client_socket, traccar_socket], [], [client_socket, traccar_socket], 0.1)
            
            for sock in readable:
                if sock == client_socket:
                    # Data from GPS device to Traccar
                    data = sock.recv(4096)
                    if not data:
                        running = False
                        break
                    
                    # Forward to Traccar
                    traccar_socket.sendall(data)
                    
                    # Add to JSON queue
                    add_to_json_queue(port, data)
                    
                    # Update last activity
                    with tcp_lock:
                        if client_id in tcp_connections:
                            tcp_connections[client_id] = (traccar_socket, time.time())
                
                elif sock == traccar_socket:
                    # Data from Traccar to GPS device
                    data = sock.recv(4096)
                    if not data:
                        running = False
                        break
                    
                    # Forward to GPS device
                    client_socket.sendall(data)
            
            # Check for socket errors
            for sock in exceptional:
                running = False
                break
    
    except Exception as e:
        logger.debug(f"TCP connection error for {client_id}: {e}")
    
    finally:
        # Clean up
        if client_socket:
            try:
                client_socket.close()
            except:
                pass
        
        if traccar_socket:
            try:
                traccar_socket.close()
            except:
                pass
        
        # Remove from connections
        with tcp_lock:
            if client_id in tcp_connections:
                del tcp_connections[client_id]

# Function to get device ID for UDP
def get_device_id(port, client_address):
    return f"UDP_{port}_{client_address[0]}_{client_address[1]}"

# Function to handle UDP data
def handle_udp_data(port, data, client_address, udp_server_socket):
    try:
        # Generate device ID
        device_id = get_device_id(port, client_address)
        
        # Add to JSON queue
        add_to_json_queue(port, data)
        
        with udp_lock:
            # Save client address for sending responses
            udp_client_addresses[device_id] = client_address
            
            # Check if connection to Traccar exists
            if device_id not in udp_connections or not udp_connections[device_id][0]:
                # Create a new connection to Traccar
                traccar_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                traccar_socket.settimeout(5)
                traccar_socket.connect((TRACCAR_HOST, DEVICE_PORTS[port]))
                traccar_socket.setblocking(0)
                udp_connections[device_id] = (traccar_socket, time.time())
            else:
                # Use existing connection
                traccar_socket, _ = udp_connections[device_id]
                # Update last activity
                udp_connections[device_id] = (traccar_socket, time.time())
        
        # Send data to Traccar
        traccar_socket.sendall(data)
        
        # Start a thread to listen for Traccar response
        worker_queue.put((listen_for_traccar_udp_response, (device_id, udp_server_socket)))
        
    except Exception as e:
        logger.debug(f"UDP handling error for device at {client_address}: {e}")
        # Clean up connection if needed
        with udp_lock:
            if device_id in udp_connections:
                try:
                    traccar_socket, _ = udp_connections[device_id]
                    traccar_socket.close()
                except:
                    pass
                udp_connections[device_id] = (None, time.time())

# Function to listen for Traccar responses to UDP devices
def listen_for_traccar_udp_response(device_id, udp_server_socket):
    try:
        with udp_lock:
            if device_id not in udp_connections or not udp_connections[device_id][0]:
                return
            
            traccar_socket, _ = udp_connections[device_id]
            
            if device_id not in udp_client_addresses:
                return
            
            client_address = udp_client_addresses[device_id]
        
        # Set a short timeout
        traccar_socket.setblocking(0)
        
        # Check for data with select
        readable, _, _ = select.select([traccar_socket], [], [], 0.5)
        
        if readable:
            response_data = traccar_socket.recv(4096)
            if response_data:
                # Send response back to UDP client
                udp_server_socket.sendto(response_data, client_address)
    
    except Exception as e:
        logger.debug(f"Error getting Traccar response for UDP device {device_id}: {e}")
        # Clean up connection
        with udp_lock:
            if device_id in udp_connections:
                try:
                    traccar_socket, _ = udp_connections[device_id]
                    traccar_socket.close()
                except:
                    pass
                udp_connections[device_id] = (None, time.time())

# Function to add data to JSON queue
def add_to_json_queue(port, data):
    try:
        # Try to decode data
        try:
            decoded_data = data.decode('utf-8', errors='replace')
        except:
            decoded_data = data.hex()
        
        # Create JSON data
        json_data = {
            "port": port,
            "data": decoded_data,
            "timestamp": time.time()
        }
        
        # Try to add to queue without blocking
        try:
            json_queue.put_nowait(json_data)
        except:
            # Queue full, log once per minute
            current_time = int(time.time())
            if current_time % 60 == 0:
                logger.warning("JSON queue full, dropping data")
    
    except Exception as e:
        logger.error(f"Error adding to JSON queue: {e}")

# Function to process JSON queue
def process_json_queue():
    while True:
        try:
            # Get item from queue with timeout
            json_data = json_queue.get(timeout=0.5)
            
            # Try to send to JSON port
            json_socket = None
            try:
                json_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                json_socket.settimeout(1)  # Short timeout
                
                # Connect to JSON port
                try:
                    json_socket.connect(("localhost", JSON_PORT))
                    
                    # Send JSON data
                    json_socket.sendall(json.dumps(json_data).encode('utf-8'))
                except socket.error:
                    # Silently fail if JSON port not available
                    pass
            
            except Exception as e:
                logger.debug(f"Error sending to JSON port: {e}")
            
            finally:
                # Close socket
                if json_socket:
                    try:
                        json_socket.close()
                    except:
                        pass
                
                # Mark task as done
                json_queue.task_done()
        
        except:
            # No items in queue, sleep briefly
            time.sleep(0.01)

# Main function
if __name__ == "__main__":
    try:
        # Initialize worker threads
        init_workers()
        
        # Start listening for data
        listen_for_data()
    
    except Exception as e:
        logger.critical(f"Critical error: {e}")