import asyncio
import socket
import json
import time
import sys
import os
import logging
import queue  # Usaremos asyncio.Queue ahora
from datetime import datetime
from collections import namedtuple

# --- Configuración ---
LISTEN_IP = "0.0.0.0"
TRACCAR_HOST = "localhost"  # Usar IP si la resolución DNS puede bloquear
JSON_FORWARD_HOST = "localhost"
JSON_FORWARD_PORT = 7005
OMIT_FILENAME = "omit_list.txt"

DEVICE_PORTS_MAP = {
    6013: 5013,  # Sinotrack (UDP/TCP)
    6001: 5001,  # Coban (TCP/UDP)
    6027: 5027,  # Teltonika (TCP)
}

# --- Gestión de Omisión ---
omit_identifiers = set()
# No necesitamos omit_lock con asyncio si la carga es desde el inicio y no dinámica

# --- Constantes y Colas Asyncio ---
BUFFER_SIZE = 4096
TCP_TIMEOUT = 180  # Timeout para inactividad en streams TCP
UDP_TIMEOUT = 300  # Timeout para relays UDP inactivos
CLEANUP_INTERVAL = 60  # Intervalo chequeo limpieza UDP
JSON_QUEUE_MAXSIZE = 10000
JSON_QUEUE = asyncio.Queue(maxsize=JSON_QUEUE_MAXSIZE)

# --- Estructuras de Datos para UDP con Estado ---
# Usaremos namedtuple para claridad
UdpRelayInfo = namedtuple(
    "UdpRelayInfo",
    ["client_address", "last_activity", "listening_transport", "traccar_transport"],
)
# Mapeos:
# client_address -> relay_info (para buscar rápido al recibir de cliente)
client_to_relay_map = {}
# traccar_transport (o un ID único si transport no es hashable) -> client_address (para buscar al recibir de Traccar)
# Usaremos el puerto efímero del transport como clave, ya que el objeto transport podría no ser estable/hashable
traccar_ephemeral_port_to_client = {}
# Lock asíncrono para proteger el acceso a los mapas anteriores
udp_map_lock = asyncio.Lock()


# --- Configuración del Logging ---
def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(
        log_dir, f"log-asyncio-{datetime.now().strftime('%Y-%m-%d')}.log"
    )
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] {%(taskName)s} %(message)s",  # Usar taskName con asyncio
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    # Poner nombre a la tarea principal
    asyncio.current_task().set_name("MainLoop")
    logging.info(
        "Logging Asyncio configurado. Nivel: DEBUG. Escribiendo a: %s", log_filename
    )


# --- Cargar Identificadores a Omitir ---
def load_omitted_identifiers(filename=OMIT_FILENAME):
    # (Misma lógica, pero usa logging)
    global omit_identifiers
    loaded_ids = set()
    try:
        with open(filename, "r", encoding="utf-8") as f:
            loaded_ids = {line.strip() for line in f if line.strip()}
        if loaded_ids:
            omit_identifiers = (
                loaded_ids  # Asignación directa (no concurrente al inicio)
            )
            logging.info(f"Cargados {len(loaded_ids)} IDs para omitir desde {filename}")
        else:
            logging.info(f"Archivo de omisión {filename} vacío.")
            omit_identifiers = set()
    except FileNotFoundError:
        logging.info(f"Archivo de omisión {filename} no encontrado.")
        omit_identifiers = set()
    except Exception as e:
        logging.error(f"Error cargando archivo de omisión {filename}: {e}")
        omit_identifiers = set()


# --- Función para verificar si se debe omitir ---
def should_omit(data):
    # (Misma lógica, usa logging)
    try:
        decoded = data.decode("utf-8", errors="ignore")
        if not omit_identifiers:
            return False
        id_found = None
        for identifier in omit_identifiers:
            if identifier in decoded:
                id_found = identifier
                break
        if id_found:
            if "tracker" in decoded.lower():
                logging.info(f"Omitiendo msg con 'tracker' del ID omitido: {id_found}")
                return True
            else:
                logging.debug(f"Msg ID omitido {id_found} permitido (sin 'tracker').")
                return False
        else:
            return False
    except:
        return False  # Seguro en caso de error


# --- Función para poner datos en la Cola JSON Asyncio ---
async def forward_to_json_port(listen_port, data, source_address, protocol="TCP"):
    """Prepara payload y lo pone en asyncio.Queue."""
    try:
        payload_for_queue = {
            "port": listen_port,
            "raw_data": data,
            "source_address_for_log": source_address,
            "protocol": protocol,
        }
        # No bloquear, poner y seguir, manejar excepción si está llena
        await JSON_QUEUE.put(payload_for_queue)
        logging.debug(
            f"-> JSON QUEUE: Datos {protocol} {source_address} p:{listen_port} encolados."
        )
    except asyncio.QueueFull:  # asyncio.Queue levanta esta excepción
        data_preview = data[:50].hex() + "..."
        logging.warning(
            f"Cola JSON llena! Descartando datos {protocol} {source_address}. Preview(hex): {data_preview}"
        )
    except Exception as e:
        logging.error(f"Error inesperado al encolar para JSON: {e}")


# --- Tarea Sender para la Cola JSON Asyncio ---
async def json_sender_task():
    """Tarea que toma datos de asyncio.Queue y los envía."""
    current_task = asyncio.current_task()
    current_task.set_name("JsonSenderTask")  # Nombrar tarea
    logging.info("Iniciando JsonSenderTask.")
    while True:
        writer = None
        payload_from_queue = None
        try:
            payload_from_queue = await JSON_QUEUE.get()  # Espera por un item
            listen_port = payload_from_queue["port"]
            raw_data = payload_from_queue["raw_data"]
            source_addr_log = payload_from_queue.get(
                "source_address_for_log", ("?", "?")
            )

            try:
                decoded_data = raw_data.decode("utf-8", errors="replace")
            except UnicodeDecodeError:
                decoded_data = raw_data.hex()

            final_json_payload = {"port": listen_port, "data": decoded_data}
            json_string = json.dumps(final_json_payload)
            encoded_json = json_string.encode("utf-8")

            # Conectar y enviar usando asyncio Streams
            try:
                # Timeout para la conexión y operaciones de stream
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(JSON_FORWARD_HOST, JSON_FORWARD_PORT),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                logging.error(
                    f"JSON SENDER: Timeout conectando a {JSON_FORWARD_HOST}:{JSON_FORWARD_PORT}"
                )
                JSON_QUEUE.task_done()
                continue  # Marcar como hecho y seguir
            except OSError as e:
                logging.error(
                    f"JSON SENDER: Error OS conectando a {JSON_FORWARD_HOST}:{JSON_FORWARD_PORT}: {e}"
                )
                JSON_QUEUE.task_done()
                continue
            except Exception as e:
                logging.error(f"JSON SENDER: Error inesperado conectando: {e}")
                JSON_QUEUE.task_done()
                continue

            logging.debug(
                f"JSON SENDER: Conectado. Enviando datos p:{listen_port} (de {source_addr_log})..."
            )
            writer.write(encoded_json)
            await writer.drain()  # Esperar que el buffer se vacíe
            logging.debug(
                f"JSON SENDER: Datos enviados p:{listen_port} (de {source_addr_log})."
            )
            JSON_QUEUE.task_done()

        except KeyError as e:
            logging.error(
                f"JSON SENDER: Error clave: {e} - Payload: {payload_from_queue}"
            )
            if payload_from_queue:
                JSON_QUEUE.task_done()
        except asyncio.CancelledError:
            logging.info("JsonSenderTask cancelada.")
            break  # Salir del bucle si la tarea es cancelada
        except Exception as e:
            logging.error(f"JSON SENDER: Error inesperado en bucle: {e}", exc_info=True)
            if payload_from_queue:
                JSON_QUEUE.task_done()  # Asegurar que la cola no se bloquee
            await asyncio.sleep(0.5)  # Pausa antes de reintentar
        finally:
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass  # Ignorar errores al cerrar


# --- Manejador para Conexiones TCP Asyncio ---
async def handle_tcp_client_async(client_reader, client_writer):
    """Maneja una conexión TCP cliente usando asyncio Streams."""
    client_address = client_writer.get_extra_info("peername")
    server_socket = client_writer.get_extra_info("socket")
    listen_port = server_socket.getsockname()[1]
    traccar_port = DEVICE_PORTS_MAP.get(listen_port)
    traccar_reader, traccar_writer = None, None
    task_name = f"TCP-H-{client_address[0]}-{client_address[1]}"
    asyncio.current_task().set_name(task_name)

    if not traccar_port:
        logging.error(f"TCP: No map port {listen_port} for {client_address}")
        client_writer.close()
        return

    logging.info(
        f"TCP: Conn {client_address} p:{listen_port} -> Traccar {TRACCAR_HOST}:{traccar_port}"
    )
    start_time = time.time()

    try:
        # Conectar a Traccar
        try:
            # Timeout para la conexión
            traccar_reader, traccar_writer = await asyncio.wait_for(
                asyncio.open_connection(TRACCAR_HOST, traccar_port), timeout=10.0
            )
        except asyncio.TimeoutError:
            logging.error(
                f"TCP: Conn timeout {client_address} -> Traccar {TRACCAR_HOST}:{traccar_port}"
            )
            return  # Salir si no podemos conectar
        except OSError as e:
            logging.error(
                f"TCP: Conn error OS {client_address} -> Traccar {TRACCAR_HOST}:{traccar_port}: {e}"
            )
            return
        except Exception as e:
            logging.error(
                f"TCP: Conn error inesperado {client_address} -> Traccar: {e}"
            )
            return

        logging.debug(f"TCP: Conn OK {client_address} <-> Traccar")

        # Crear tareas para leer de cada lado y reenviar
        client_to_traccar_task = asyncio.create_task(
            forward_stream(
                client_reader, traccar_writer, client_address, "Client", listen_port
            ),
            name=f"{task_name}-CliToSrv",
        )
        traccar_to_client_task = asyncio.create_task(
            forward_stream(
                traccar_reader,
                client_writer,
                client_address,
                "Traccar",
                listen_port,
                is_traccar_source=True,
            ),
            name=f"{task_name}-SrvToCli",
        )

        # Esperar a que CUALQUIERA de las tareas termine (indica cierre o error)
        done, pending = await asyncio.wait(
            [client_to_traccar_task, traccar_to_client_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Cancelar la tarea pendiente (si la hay)
        for task in pending:
            logging.debug(f"Cancelando tarea pendiente: {task.get_name()}")
            task.cancel()
            try:
                await task  # Esperar que la cancelación se procese
            except asyncio.CancelledError:
                logging.debug(f"Tarea {task.get_name()} cancelada exitosamente.")
            except Exception as e_cancel:
                logging.error(
                    f"Error esperando cancelación de {task.get_name()}: {e_cancel}"
                )

    except Exception as e:
        logging.error(
            f"TCP: Error general en handle_tcp_client_async para {client_address}: {e}",
            exc_info=True,
        )
    finally:
        logging.debug(f"TCP: Limpiando conexión para {client_address}")
        if client_writer:
            try:
                client_writer.close()
                await client_writer.wait_closed()
            except:
                pass
        if traccar_writer:
            try:
                traccar_writer.close()
                await traccar_writer.wait_closed()
            except:
                pass
        duration = time.time() - start_time
        logging.info(
            f"TCP: Conn ended {client_address} p:{listen_port}. Dur: {duration:.2f}s"
        )


async def forward_stream(
    reader, writer, client_addr, source_name, listen_port, is_traccar_source=False
):
    """Lee de un reader y escribe a un writer, manejando cierre y errores."""
    peer_name = writer.get_extra_info("peername", "Desconocido")
    while True:
        try:
            # Aplicar timeout a la lectura
            data = await asyncio.wait_for(reader.read(BUFFER_SIZE), timeout=TCP_TIMEOUT)
            if not data:
                logging.info(
                    f"TCP Forward: Conexión cerrada por {source_name} para {client_addr}. Terminando forward."
                )
                break  # Salir del bucle si la conexión se cierra limpiamente

            logging.debug(
                f"TCP Forward: {source_name} ({client_addr}) -> Peer {peer_name} ({len(data)} bytes)"
            )

            # Si los datos vienen del cliente, verificar omisión y encolar para JSON
            if not is_traccar_source:
                if should_omit(data):
                    logging.debug(
                        f"TCP Forward: Datos de {source_name} ({client_addr}) OMITIDOS."
                    )
                    continue  # No reenviar ni encolar

                # Encolar para JSON ANTES de escribir (por si la escritura falla)
                await forward_to_json_port(listen_port, data, client_addr, "TCP")

            # Escribir los datos al otro stream
            writer.write(data)
            await writer.drain()  # Esperar a que se envíe

        except asyncio.TimeoutError:
            logging.info(
                f"TCP Forward: Timeout ({TCP_TIMEOUT}s) leyendo de {source_name} para {client_addr}. Cerrando."
            )
            break
        except asyncio.CancelledError:
            logging.info(
                f"TCP Forward: Tarea cancelada leyendo de {source_name} para {client_addr}."
            )
            break  # Salir si la tarea es cancelada externamente
        except ConnectionResetError:
            logging.warning(
                f"TCP Forward: Conexión reseteada leyendo de {source_name} para {client_addr}."
            )
            break
        except OSError as e:
            logging.error(
                f"TCP Forward: Error OS leyendo/escribiendo ({source_name} para {client_addr}): {e}"
            )
            break
        except Exception as e:
            logging.error(
                f"TCP Forward: Error inesperado ({source_name} para {client_addr}): {e}",
                exc_info=True,
            )
            break
    # Asegurarse de que el writer se cierre al salir del forwarder podría ser redundante
    # si handle_tcp_client_async ya lo cierra, pero puede ser una salvaguarda.
    # try:
    #     if not writer.is_closing(): writer.close(); await writer.wait_closed()
    # except: pass


# --- Protocolo UDP Asyncio ---
class UdpProxyProtocol(asyncio.DatagramProtocol):
    """Protocolo para manejar datagramas UDP entrantes y salientes."""

    def __init__(self, listen_port):
        self.listen_port = listen_port
        self.traccar_port = DEVICE_PORTS_MAP.get(listen_port)
        self.transport = None  # El transporte del socket de escucha original
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport
        sock = transport.get_extra_info("socket")
        logging.info(
            f"UDP: Escuchando en {sock.getsockname()} -> Traccar {TRACCAR_HOST}:{self.traccar_port}"
        )

    def connection_lost(self, exc):
        logging.warning(
            f"UDP: Transporte de escucha perdido para puerto {self.listen_port}. Razón: {exc}"
        )
        # Aquí podríamos necesitar lógica para reintentar la escucha si es deseable

    def datagram_received(self, data, addr):
        """Se llama cuando se recibe un datagrama en el socket de escucha."""
        # Lanzar esto como una tarea para no bloquear el receptor de datagramas
        asyncio.create_task(
            self.handle_datagram_from_client(data, addr),
            name=f"UDP-H-{addr[0]}-{addr[1]}",
        )

    async def handle_datagram_from_client(self, data, client_address):
        """Procesa un datagrama UDP recibido de un dispositivo."""
        if not self.traccar_port:
            logging.warning(
                f"UDP: Puerto {self.listen_port} sin mapeo para {client_address}. Ignorando."
            )
            return

        logging.debug(
            f"UDP: IN <= Cli {client_address} p:{self.listen_port} ({len(data)}b)"
        )
        if should_omit(data):
            logging.debug(f"UDP: OMIT {client_address}")
            return

        # Encolar para JSON
        await forward_to_json_port(self.listen_port, data, client_address, "UDP")

        relay_transport_to_use = None
        now = time.time()

        # --- Sección Crítica UDP ---
        async with udp_map_lock:
            if client_address in client_to_relay_map:  # Buscar relay existente
                existing_relay_info = client_to_relay_map[client_address]
                # Verificar si el transport aún existe/está activo (difícil de saber directamente)
                # Por ahora, confiamos en que el cleanup lo maneja. Actualizar timestamp.
                client_to_relay_map[client_address] = existing_relay_info._replace(
                    last_activity=now
                )
                relay_transport_to_use = existing_relay_info.traccar_transport
                ephemeral_port = relay_transport_to_use.get_extra_info(
                    "socket"
                ).getsockname()[1]
                logging.debug(
                    f"UDP: Usando relay existente via :{ephemeral_port} para {client_address}"
                )
            else:  # Crear nuevo relay
                logging.debug(f"UDP: Creando NUEVO relay para {client_address}")
                try:
                    loop = asyncio.get_running_loop()
                    # Crear un nuevo endpoint/transporte para hablar con Traccar
                    traccar_transport, _ = await loop.create_datagram_endpoint(
                        lambda: UdpRelayProtocol(
                            client_address, self.transport
                        ),  # Pasa la dirección cliente y el transport de escucha original
                        remote_addr=(
                            TRACCAR_HOST,
                            self.traccar_port,
                        ),  # Conectar directamente a Traccar
                        # No usamos local_addr aquí, dejamos que el sistema elija puerto efímero
                    )
                    relay_transport_to_use = traccar_transport
                    ephemeral_port = traccar_transport.get_extra_info(
                        "socket"
                    ).getsockname()[1]

                    # Guardar info del nuevo relay
                    new_relay_info = UdpRelayInfo(
                        client_address=client_address,
                        last_activity=now,
                        listening_transport=self.transport,  # Guardamos el transport de escucha
                        traccar_transport=relay_transport_to_use,
                    )
                    client_to_relay_map[client_address] = new_relay_info
                    # Mapeo inverso usando el puerto efímero como clave
                    traccar_ephemeral_port_to_client[ephemeral_port] = client_address

                    logging.info(
                        f"UDP: Nuevo relay via :{ephemeral_port} creado para {client_address}. Total: {len(client_to_relay_map)}"
                    )

                except OSError as e:
                    logging.error(
                        f"UDP: Error OS creando relay endpoint para {client_address}: {e}"
                    )
                except Exception as e:
                    logging.error(
                        f"UDP: Error inesperado creando relay endpoint para {client_address}: {e}",
                        exc_info=True,
                    )
        # --- Fin Sección Crítica UDP ---

        if relay_transport_to_use:
            try:
                relay_transport_to_use.sendto(data)  # Ya está "conectado" a Traccar
                logging.debug(
                    f"UDP: OUT => Traccar via :{ephemeral_port} para {client_address}"
                )
            except Exception as e:
                logging.error(
                    f"UDP: Error sendto Traccar para {client_address} via :{ephemeral_port}: {e}"
                )
                # Podríamos intentar cerrar/limpiar este relay si falla el envío

    def error_received(self, exc):
        # Se llama cuando una operación send() o recv() anterior levanta un OSError.
        # Ej. ICMP "host unreachable"
        logging.error(
            f"UDP: Error async en transporte de escucha puerto {self.listen_port}: {exc}"
        )
        # Podríamos necesitar limpiar relays asociados a este error si es posible identificarlo


class UdpRelayProtocol(asyncio.DatagramProtocol):
    """Protocolo simple para manejar respuestas de Traccar en sockets relay."""

    def __init__(self, client_address, listening_transport):
        self.client_address = client_address  # A quién reenviar la respuesta
        self.listening_transport = (
            listening_transport  # Qué transporte usar para reenviar
        )
        self.transport = None
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport  # El transporte de este relay
        sock = transport.get_extra_info("socket")
        logging.debug(
            f"UDP Relay: Transporte {sock.getsockname()} conectado a Traccar para cliente {self.client_address}"
        )

    def connection_lost(self, exc):
        # El relay se cerró (por timeout cleanup o error)
        sock_name = (
            self.transport.get_extra_info("socket").getsockname()
            if self.transport
            else "desconocido"
        )
        logging.debug(
            f"UDP Relay: Conexión perdida para {sock_name} (cliente {self.client_address}). Razón: {exc}"
        )
        # La limpieza principal se hace en el cleanup_task

    def datagram_received(self, data, addr):
        """Se llama cuando Traccar envía datos de vuelta a este relay."""
        logging.debug(
            f"UDP Relay: Respuesta IN <= Traccar {addr} via {self.transport.get_extra_info('socket').getsockname()} para {self.client_address} ({len(data)}b)"
        )

        # Actualizar timestamp (necesita lock)
        async def update_activity():
            async with udp_map_lock:
                if self.client_address in client_to_relay_map:
                    relay_info = client_to_relay_map[self.client_address]
                    # Solo actualizar si el transporte coincide (seguridad extra)
                    if relay_info.traccar_transport == self.transport:
                        client_to_relay_map[self.client_address] = relay_info._replace(
                            last_activity=time.time()
                        )
                    else:
                        logging.warning(
                            f"UDP Relay: Discrepancia de transporte al actualizar actividad para {self.client_address}"
                        )
                # else: el relay ya fue limpiado

        asyncio.create_task(
            update_activity(), name=f"UDP-UpdateAct-{self.client_address[0]}"
        )

        # Reenviar al cliente original usando el transporte de escucha
        try:
            if self.listening_transport and not self.listening_transport.is_closing():
                self.listening_transport.sendto(data, self.client_address)
                logging.debug(
                    f"UDP Relay: Respuesta OUT => Cliente {self.client_address} desde {self.listening_transport.get_extra_info('socket').getsockname()}"
                )
            else:
                logging.warning(
                    f"UDP Relay: Transporte de escucha para {self.client_address} no disponible o cerrándose. No se pudo reenviar respuesta."
                )
        except Exception as e:
            logging.error(
                f"UDP Relay: Error reenviando respuesta a {self.client_address}: {e}"
            )

    def error_received(self, exc):
        # Error en este transporte relay (ej. error al enviar a Traccar)
        sock_name = (
            self.transport.get_extra_info("socket").getsockname()
            if self.transport
            else "desconocido"
        )
        logging.error(
            f"UDP Relay: Error async en transporte {sock_name} (cliente {self.client_address}): {exc}"
        )

        # Podríamos cerrar y limpiar este relay aquí mismo
        async def cleanup_self():
            logging.warning(
                f"UDP Relay: Limpiando relay para {self.client_address} debido a error_received."
            )
            async with udp_map_lock:
                ephemeral_port = self.transport.get_extra_info("socket").getsockname()[
                    1
                ]
                client_addr_check = traccar_ephemeral_port_to_client.pop(
                    ephemeral_port, None
                )
                if client_addr_check == self.client_address:  # Verificar consistencia
                    client_to_relay_map.pop(self.client_address, None)
                else:
                    logging.warning(
                        f"UDP Relay Cleanup(error): Discrepancia de mapeo para puerto {ephemeral_port}"
                    )
            if self.transport:
                self.transport.close()  # Cerrar el transporte

        asyncio.create_task(
            cleanup_self(), name=f"UDP-CleanupErr-{self.client_address[0]}"
        )


# --- Tarea de Limpieza UDP Asyncio ---
async def cleanup_inactive_udp_relays_task():
    """Tarea periódica para limpiar relays UDP inactivos."""
    current_task = asyncio.current_task()
    current_task.set_name("UdpCleanupTask")  # Nombrar tarea
    logging.info("Iniciando UdpCleanupTask.")
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        now = time.time()
        cleaned_count = 0
        relays_to_cleanup = []  # (ephemeral_port, client_address, traccar_transport)

        # --- Sección Crítica UDP ---
        async with udp_map_lock:
            # Iterar sobre copia de keys del mapeo inverso (puerto efímero -> cliente)
            current_ephemeral_ports = list(traccar_ephemeral_port_to_client.keys())
            for ephemeral_port in current_ephemeral_ports:
                client_address = traccar_ephemeral_port_to_client.get(ephemeral_port)
                if client_address and client_address in client_to_relay_map:
                    relay_info = client_to_relay_map[client_address]
                    if now - relay_info.last_activity > UDP_TIMEOUT:
                        logging.info(
                            f"UDP Cleanup: Timeout para relay via :{ephemeral_port} de {client_address}. Marcando para limpieza."
                        )
                        # Guardar info necesaria para cerrar fuera del lock
                        relays_to_cleanup.append(
                            (
                                ephemeral_port,
                                client_address,
                                relay_info.traccar_transport,
                            )
                        )
                        # Eliminar de los mapeos AHORA dentro del lock
                        del traccar_ephemeral_port_to_client[ephemeral_port]
                        del client_to_relay_map[client_address]
                        cleaned_count += 1
                elif (
                    client_address
                ):  # Mapeo puerto->cliente existe, pero cliente->relay no? Limpiar puerto->cliente
                    logging.warning(
                        f"UDP Cleanup: Mapeo inconsistente encontrado para puerto {ephemeral_port}. Limpiando."
                    )
                    del traccar_ephemeral_port_to_client[ephemeral_port]
                # else: El puerto ya fue eliminado, no hacer nada
        # --- Fin Sección Crítica UDP ---

        # Cerrar los transportes fuera del lock
        if relays_to_cleanup:
            logging.info(
                f"UDP Cleanup: Cerrando {len(relays_to_cleanup)} transportes relay inactivos."
            )
            for _, _, traccar_transport in relays_to_cleanup:
                try:
                    if traccar_transport and not traccar_transport.is_closing():
                        traccar_transport.close()
                except Exception as e_close:
                    logging.error(f"UDP Cleanup: Error cerrando transporte: {e_close}")

        if cleaned_count > 0:
            logging.info(
                f"UDP Cleanup: {cleaned_count} relays inactivos procesados para cierre."
            )
        async with udp_map_lock:
            active_count = len(client_to_relay_map)  # Estado actual
        logging.debug(f"UDP Cleanup: Relays activos estimados: {active_count}")


# --- Función Principal Asyncio ---
async def main():
    """Función principal que inicia servidores y tareas."""
    setup_logging()
    load_omitted_identifiers()
    loop = asyncio.get_running_loop()

    # Iniciar tareas en segundo plano
    json_sender = asyncio.create_task(json_sender_task())
    udp_cleaner = asyncio.create_task(cleanup_inactive_udp_relays_task())

    servers = []  # Guardar referencias a los objetos Server

    # Crear servidores TCP y UDP para cada puerto mapeado
    for listen_port, _ in DEVICE_PORTS_MAP.items():
        try:
            # Servidor TCP
            tcp_server = await loop.create_server(
                lambda: asyncio.StreamReaderProtocol(
                    asyncio.StreamReader(), handle_tcp_client_async
                ),
                LISTEN_IP,
                listen_port,
                family=socket.AF_INET,  # Asegurar IPv4 si es necesario
            )
            servers.append(tcp_server)
            logging.info(
                f"Servidor TCP escuchando en {tcp_server.sockets[0].getsockname()}"
            )

            # Servidor UDP (Endpoint)
            # Pasamos el puerto de escucha a la factoría del protocolo
            await loop.create_datagram_endpoint(
                lambda lp=listen_port: UdpProxyProtocol(lp),
                local_addr=(LISTEN_IP, listen_port),
                family=socket.AF_INET,
            )
            # No guardamos el transporte de escucha UDP aquí, se maneja en el protocolo

        except OSError as e:
            logging.critical(
                f"Fallo al iniciar servidor en puerto {listen_port}: {e}. Saliendo."
            )
            # Cerrar servidores ya iniciados y cancelar tareas
            for server in servers:
                server.close()
            await asyncio.gather(
                *[s.wait_closed() for s in servers], return_exceptions=True
            )
            json_sender.cancel()
            udp_cleaner.cancel()
            await asyncio.gather(json_sender, udp_cleaner, return_exceptions=True)
            return  # Salir de main()
        except Exception as e:
            logging.critical(
                f"Error inesperado iniciando servidor en puerto {listen_port}: {e}",
                exc_info=True,
            )
            # Misma lógica de limpieza anterior
            for server in servers:
                server.close()
            await asyncio.gather(
                *[s.wait_closed() for s in servers], return_exceptions=True
            )
            json_sender.cancel()
            udp_cleaner.cancel()
            await asyncio.gather(json_sender, udp_cleaner, return_exceptions=True)
            return

    logging.info(f"Todos los servidores iniciados. Proxy listo.")

    # Mantener la función principal corriendo (o esperar tareas si es apropiado)
    # En este caso, los servidores corren "para siempre" hasta que se interrumpa
    # Podríamos esperar aquí a que todas las tareas terminen si tuvieran una condición de salida
    # await asyncio.gather(json_sender, udp_cleaner)
    # O simplemente dormir indefinidamente hasta Ctrl+C
    try:
        while True:
            await asyncio.sleep(3600)  # Dormir por mucho tiempo
    except asyncio.CancelledError:
        logging.info("Bucle principal cancelado.")
    finally:
        logging.info("Iniciando cierre de servidores TCP...")
        for server in servers:
            server.close()
        await asyncio.gather(
            *[s.wait_closed() for s in servers], return_exceptions=True
        )
        logging.info("Servidores TCP cerrados.")
        # Cancelar tareas si aún corren
        if not json_sender.done():
            json_sender.cancel()
        if not udp_cleaner.done():
            udp_cleaner.cancel()
        logging.info("Esperando finalización de tareas...")
        await asyncio.gather(json_sender, udp_cleaner, return_exceptions=True)
        logging.info("Tareas finalizadas.")
        # El event loop se cerrará solo al salir de `asyncio.run()`


# --- Punto de Entrada ---
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Cierre solicitado por usuario (Ctrl+C) detectado.")
    except Exception as e:
        logging.critical(
            f"Error fatal no capturado ejecutando main: {e}", exc_info=True
        )

    logging.info("Programa terminado.")
