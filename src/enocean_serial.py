"""Raw EnOcean ESP3 serial communication layer.

Replaces the enocean library's SerialCommunicator with a reliable raw serial
implementation. Handles CRC8, packet framing, send/receive, and Base ID reading.
"""

import asyncio
import logging
import threading
import time
from collections.abc import Callable
from typing import TypedDict

import serial

logger = logging.getLogger(__name__)

# ESP3 constants
SYNC_BYTE = 0x55
PACKET_TYPE_RADIO = 0x01
PACKET_TYPE_RESPONSE = 0x02
PACKET_TYPE_COMMON_COMMAND = 0x05
CO_RD_IDBASE = 0x08
RETURN_CODE_OK = 0x00

# RORG types
RORG_RPS = 0xF6
RORG_MSC = 0xD1


class RadioPacket(TypedDict):
    rorg: int
    sender: list[int]
    status: int
    user_data: list[int]
    dest: list[int] | None


def crc8(data: list[int] | bytes) -> int:
    """Calculate CRC8 with polynomial 0x07 (EnOcean ESP3)."""
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x80:
                crc = (crc << 1) ^ 0x07
            else:
                crc = crc << 1
            crc &= 0xFF
    return crc


def build_esp3_packet(data: list[int], optional: list[int], packet_type: int = PACKET_TYPE_RADIO) -> bytes:
    """Build a complete ESP3 packet with sync, header, CRCs."""
    header = [SYNC_BYTE, 0x00, len(data), len(optional), packet_type]
    header_crc = crc8(header[1:5])
    data_crc = crc8(data + optional)
    return bytes(header + [header_crc] + data + optional + [data_crc])


class ParseResult:
    """Result of ESP3 packet parsing, tracking consumed bytes."""
    __slots__ = ('packets', 'consumed')

    def __init__(self) -> None:
        self.packets: list[dict] = []
        self.consumed: int = 0


def parse_esp3_packets(raw: bytes) -> ParseResult:
    """Parse raw bytes into ESP3 packets.

    Returns ParseResult with parsed packets and number of bytes safely consumed.
    Bytes after `consumed` may contain an incomplete packet and must be kept
    in the buffer for the next read cycle.
    """
    result = ParseResult()
    data = list(raw)
    i = 0
    last_good = 0  # track last position where we know all prior bytes are consumed

    while i < len(data):
        if data[i] != SYNC_BYTE:
            i += 1
            last_good = i
            continue

        if i + 6 > len(data):
            # Could be start of a packet but not enough header bytes yet
            break

        data_len = (data[i + 1] << 8) | data[i + 2]
        opt_len = data[i + 3]
        pkt_type = data[i + 4]
        header_crc_pos = i + 5

        # Sanity check: reject absurd lengths (max ESP3 data = 65535, but
        # MAICO MSC packets are always small — cap at 256 to catch corruption)
        if data_len > 256 or opt_len > 256:
            i += 1
            last_good = i
            continue

        # Total packet length: sync(1) + header(4) + header_crc(1) + data + optional + data_crc(1)
        pkt_total = 1 + 4 + 1 + data_len + opt_len + 1
        if i + pkt_total > len(data):
            # Incomplete packet — keep remaining bytes for next read
            break

        # Verify header CRC
        expected_hcrc = crc8(data[i + 1:i + 5])
        if data[header_crc_pos] != expected_hcrc:
            i += 1
            last_good = i
            continue

        # Extract data and optional sections
        data_start = i + 6
        data_end = data_start + data_len
        opt_start = data_end
        opt_end = opt_start + opt_len
        data_crc_pos = opt_end

        # Verify data CRC
        expected_dcrc = crc8(data[data_start:opt_end])
        if data[data_crc_pos] != expected_dcrc:
            i += 1
            last_good = i
            continue

        pkt_data = data[data_start:data_end]
        pkt_opt = data[opt_start:opt_end]

        if pkt_type == PACKET_TYPE_RADIO and data_len > 6:
            # RADIO_ERP1: RORG + user_data + sender(4) + status(1)
            rorg = pkt_data[0]
            sender = pkt_data[-5:-1]
            status = pkt_data[-1]
            user_data = pkt_data[1:-5]
            dest = pkt_opt[1:5] if opt_len >= 5 else None
            result.packets.append({
                'type': 'radio',
                'rorg': rorg,
                'sender': sender,
                'status': status,
                'user_data': user_data,
                'dest': dest,
            })
        elif pkt_type == PACKET_TYPE_RESPONSE:
            result.packets.append({
                'type': 'response',
                'data': pkt_data,
            })

        i += pkt_total
        last_good = i

    result.consumed = last_good
    return result


class EnOceanSerial:
    """Raw serial interface to EnOcean USB 300 stick."""

    RECONNECT_DELAY = 5  # seconds between reconnect attempts

    def __init__(self, port: str, baudrate: int = 57600):
        self._port = port
        self._baudrate = baudrate
        self._ser: serial.Serial | None = None
        self._base_id: list[int] | None = None
        self._running = False
        # Serialize writes: send() is called from the asyncio loop (poll loop,
        # web handlers) and historically from the paho MQTT thread. Two writes
        # interleaving on the wire would corrupt ESP3 frames on the RF channel.
        self._write_lock = threading.Lock()
        self._read_thread: threading.Thread | None = None
        self._packet_queue: asyncio.Queue[dict] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._on_packet: Callable[[dict], None] | None = None

    @property
    def base_id(self) -> list[int] | None:
        return self._base_id

    @property
    def base_id_str(self) -> str:
        if self._base_id:
            return ":".join(f"{b:02X}" for b in self._base_id)
        return "unknown"

    def open(self) -> None:
        """Open serial port and read base ID."""
        logger.info("Opening serial port: %s @ %d baud", self._port, self._baudrate)
        try:
            self._ser = serial.Serial(self._port, baudrate=self._baudrate, timeout=0.5)
            self._ser.reset_input_buffer()
            self._read_base_id()
        except serial.SerialException:
            logger.exception("Failed to open serial port %s", self._port)
            raise
        except OSError:
            logger.exception("Serial port %s not accessible", self._port)
            raise

    def close(self) -> None:
        """Close serial port."""
        self._running = False
        # Close serial first to unblock any read() in the thread
        try:
            if self._ser and self._ser.is_open:
                self._ser.close()
                logger.info("Serial port closed")
        except Exception:
            logger.debug("Error closing serial port", exc_info=True)
        if self._read_thread and self._read_thread.is_alive():
            self._read_thread.join(timeout=3)
            if self._read_thread.is_alive():
                logger.warning("Serial read thread did not exit cleanly")

    def send(self, data: list[int], optional: list[int]) -> None:
        """Send an ESP3 RADIO_ERP1 packet."""
        if not self._ser or not self._ser.is_open:
            logger.error("Cannot send: serial port not open")
            return
        try:
            packet = build_esp3_packet(data, optional)
            with self._write_lock:
                self._ser.write(packet)
            logger.debug("TX: %s", packet.hex(' '))
        except serial.SerialException:
            logger.exception("Serial write failed")

    def send_common_command(self, command_data: list[int]) -> None:
        """Send an ESP3 common command."""
        if not self._ser or not self._ser.is_open:
            return
        try:
            packet = build_esp3_packet(command_data, [], PACKET_TYPE_COMMON_COMMAND)
            with self._write_lock:
                self._ser.write(packet)
        except serial.SerialException:
            logger.exception("Serial command write failed")

    def _read_base_id(self) -> None:
        """Read base ID from stick via CO_RD_IDBASE."""
        if not self._ser:
            return

        # Send CO_RD_IDBASE
        self.send_common_command([CO_RD_IDBASE])

        # Wait for response
        time.sleep(0.5)
        try:
            if self._ser.in_waiting:
                raw = self._ser.read(self._ser.in_waiting)
                for pkt in parse_esp3_packets(raw).packets:
                    if pkt.get('type') == 'response' and len(pkt['data']) >= 5:
                        if pkt['data'][0] == RETURN_CODE_OK:
                            self._base_id = list(pkt['data'][1:5])
                            logger.info("Base ID: %s", self.base_id_str)
                            return
        except serial.SerialException:
            logger.exception("Failed to read base ID")

        logger.warning("Could not read base ID from stick")

    async def start_receive_loop(self) -> None:
        """Start the async receive loop. Call from asyncio context."""
        self._loop = asyncio.get_running_loop()
        self._packet_queue = asyncio.Queue()
        self._running = True

        # Start background read thread
        self._read_thread = threading.Thread(target=self._serial_read_thread, daemon=True)
        self._read_thread.start()

        # Process packets from queue
        while self._running:
            try:
                pkt = await asyncio.wait_for(self._packet_queue.get(), timeout=0.1)
                if self._on_packet:
                    try:
                        self._on_packet(pkt)
                    except Exception:
                        logger.exception("Error in packet callback")
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

    def on_packet(self, callback: Callable[[dict], None]) -> None:
        """Register callback for received packets."""
        self._on_packet = callback

    def _serial_read_thread(self) -> None:
        """Background thread that reads from serial and pushes to async queue."""
        buf = bytearray()
        while self._running:
            try:
                if not self._ser or not self._ser.is_open:
                    break
                if self._ser.in_waiting:
                    chunk = self._ser.read(self._ser.in_waiting)
                    buf.extend(chunk)

                    # Try to parse complete packets, keeping incomplete trailing data
                    result = parse_esp3_packets(bytes(buf))
                    if result.consumed > 0:
                        del buf[:result.consumed]
                    # Safety: prevent unbounded buffer growth from unparseable data
                    if len(buf) > 4096:
                        logger.warning("Serial buffer overflow (%d bytes), clearing", len(buf))
                        buf.clear()
                    for pkt in result.packets:
                        try:
                            loop = self._loop
                            queue = self._packet_queue
                            if loop and queue:
                                loop.call_soon_threadsafe(queue.put_nowait, pkt)
                            else:
                                logger.debug("Packet dropped: no event loop or queue")
                        except RuntimeError:
                            logger.debug("Packet dropped: event loop closed")
                else:
                    time.sleep(0.02)
            except serial.SerialException:
                if self._running:
                    logger.exception("Serial read error")
                break
            except Exception:
                logger.exception("Error in serial read thread")
                time.sleep(0.1)

        logger.info("Serial read thread stopped")
