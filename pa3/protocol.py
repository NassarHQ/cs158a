import base64
import binascii
import json
import logging
import socket
import struct
import threading

# CONSTANTS
UDP_PORT = 54321 # Default UDP port
HEADER_SIZE = 4 # Bytes for length prefix
HEADER_FORMAT = '>I' # Big-endian unsigned int
MAX_MSG_SIZE = 50 * 1024 * 1024 # 50 MB
CHUNK_SIZE = 4096 # 4 KB
UDP_BUFFER_SIZE = 65535 # Max UDP payload

# Message Types
PEER_REQUEST = "peer_request"
PEER_ACK = "peer_ack"
FILE_LIST = "file_list"
DATA_REQUEST = "data_request"
FILE_DATA = "file_data"
ERROR = "error"

REQUIRED_FIELDS = {
    PEER_REQUEST: {"node_id"},
    PEER_ACK: {"node_id", "tcp_port"},
    FILE_LIST: {"files"},
    DATA_REQUEST: {"filename"},
    FILE_DATA: {"filename", "content"},
    ERROR: {"filename", "reason"},
}

class ProtocolError(Exception):
    """
    Peer broke the protocol: bad JSON, unknown type, missing fields,
    or a length over MAX_MSG_SIZE.
    """

def make_peer_request(node_id):
    """
    Build PEER_REQUEST message
    """
    return {"type": PEER_REQUEST, "node_id": node_id}

def make_peer_ack(node_id, tcp_port):
    """
    Build PEER_ACK message
    """
    return {"type": PEER_ACK, "node_id": node_id, "tcp_port": tcp_port}

def make_file_list(filenames):
    """
    Build FILE_LIST message.
    """
    return {"type": FILE_LIST, "files": list(filenames)}

def make_data_request(filename):
    """
    Build DATA_REQUEST message
    """
    return {"type": DATA_REQUEST, "filename": filename}

def make_file_data(filename, raw_bytes):
    """
    Build FILE_DATA message
    """
    encoded = base64.b64encode(raw_bytes).decode('ascii')
    return {"type": FILE_DATA, "filename": filename, "content": encoded}

def make_error(filename, reason):
    """
    Build ERROR_REPLY
    """
    return {"type": ERROR, "filename": filename, "reason": reason}

def decode_file_content(msg):
    """
    Turn FILE_DATA message's base64 "content" back into raw bytes
    """
    try:
        return base64.b64decode(msg["content"], validate=True)
    except (binascii.Error, ValueError) as e:
        raise ProtocolError(f"Invalid base64 in file_data: {e}") from e

# Encoding & Validation (shared by UDP and TCP)

def encode_message(msg):
    """
    dict -> bytes
    json.dumps produces a str, encode ('utf-8') turn it into bytes
    """
    return json.dumps(msg).encode('utf-8')

def decode_message(payload):
    """
    Order Check:
    1. Bytes are valid UTF-8 and valid JSON
    2. the JSON is an object (dict), not a list/number/string/null
    3. the "type" is one we know
    4. every field that type requires is present
    """
    try: 
        msg = json.loads(payload.decode('utf-8'))
    except ValueError as e:
        raise ProtocolError(f"Malformed payload: {e}") from e
    
    if not isinstance(msg, dict):
        raise ProtocolError("Message is not a JSON object")
    
    msg_type = msg.get("type")
    if msg_type not in REQUIRED_FIELDS:
        raise ProtocolError(f"Unknown message type: {msg_type!r}")
    
    missing = REQUIRED_FIELDS[msg_type] - msg.keys()
    if missing:
        raise ProtocolError(f"{msg_type} missing fields: {sorted(missing)}")

    return msg

# Logging MODULE (THREAD-SAFE)
logger = logging.getLogger("p2p")

def setup_logging(path="log.txt"):
    """Send all log lines to log.txt with a timestamp call once at startup."""
    handler = logging.FileHandler(path)
    handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def _fmt_addr(addr):
    """Turn a sockaddr into a concise '[IP]:[PORT]' string."""
    if isinstance(addr, tuple) and len(addr) >= 2:
        return f"{addr[0]}:{addr[1]}"
    return str(addr)

def log_message(direction, peer_addr, msg_type):
    """Log one sent or received message: direction, type, and peer address."""
    logger.info(f"{direction:<9} {msg_type:<13} {_fmt_addr(peer_addr)}")

def log_peer_change(action, peer_addr):
    """Log a peer table change: 'PEER ADDED' or 'PEER REMOVED'."""
    logger.info(f"{action:<23} {_fmt_addr(peer_addr)}")

def send_msg(sock, msg, peer_addr, send_lock):
    """Send one TCP message as [4-byte length][JSON], holding the peer's lock."""

    payload = encode_message(msg)

    # Refuse to send something the receiver would reject anyway
    if len(payload) > MAX_MSG_SIZE:
        raise ProtocolError(
            f"Message too large: {len(payload)} bytes > {MAX_MSG_SIZE}"
        )

    # Integer length -> exactly 4 bytes
    header = struct.pack(HEADER_FORMAT, len(payload))

    # Only one thread sends on this socket at a time
    with send_lock:
        sock.sendall(header + payload)

    log_message("SENT", peer_addr, msg["type"])

def recv_msg(sock, peer_addr):
    """Read one TCP message; return a dict, or None if the peer disconnected."""
    # Read exactly 4 bytes, the length prefix
    header = _recv_all(sock, HEADER_SIZE)
    if header is None:
        return None
    
    # Decode the length. Unpack always returns a tuple, so it pulls out its single element.
    (msg_length,) = struct.unpack(HEADER_FORMAT, header)

    if msg_length > MAX_MSG_SIZE:
        raise ProtocolError(
            f"Declared length {msg_length} exceeds max {MAX_MSG_SIZE}"
        )
    
    # Read exactly msg_length bytes of payload
    payload = _recv_all(sock, msg_length)
    if payload is None:
        return None
    
    # Bytes -> validated dict (raise error if bad)
    msg = decode_message(payload)
    log_message("RECEIVED", peer_addr, msg["type"])
    return msg

def _recv_all(sock, n):
    """Read exactly n bytes from the socket, or return None if the peer disconnects."""
    # bytearray is mutable
    data = bytearray()

    while len(data) < n:
        remaining = n - len(data)
        try:
            chunk = sock.recv(min(CHUNK_SIZE, remaining))
        except OSError:
            return None

        if not chunk:
            return None
        
        data.extend(chunk)
    
    return bytes(data)

def send_udp(sock, msg, addr):
    """Send one message as a single UDP datagram to addr."""
    sock.sendto(encode_message(msg), addr)
    log_message("SENT", addr, msg["type"])

def recv_udp(sock):
    """Wait for one UDP datagram, return (msg, addr), with msg None if invalid."""
    data, addr = sock.recvfrom(UDP_BUFFER_SIZE)
    try:
        msg = decode_message(data)
    except ProtocolError:
        return None, addr
    log_message("RECEIVED", addr, msg["type"])
    return msg, addr