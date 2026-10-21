"""
P2P file sharing agent for CS158A PA3

Usage: python3 agent.py <tcp_port> [node_dir]
node_dir holds this node's shared/, downloads/ and log.txt (default: .)

Commands:
  list            peer table: each peer and the files it advertises
  files           files in own shared/
  get <filename>  download a file from a peer that has it
  quit            close all sockets and exit
"""

import os
import shutil
import socket
import sys
import threading
import time
import uuid

import protocol as p

BROADCAST_ADDR = ("<broadcast>", p.UDP_PORT)
FILE_LIST_INTERVAL = 10 # seconds
CONNECT_TIMEOUT = 5 # seconds


def close_socket(sock):
    """Shut down and close a socket, ignore errors if already closed."""
    # shutdown also wakes a thread blocked in recv/accept on this socket
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()


class Peer:
    """One TCP connection to another node, plus the files it advertises."""

    def __init__(self, sock, addr, outbound, node_id=None):
        self.sock = sock
        self.addr = addr # (ip, port) of the other end
        self.outbound = outbound # True if this side dialed
        self.node_id = node_id # from the ack, or from its first file list
        self.files = [] # last advertised file list
        self.send_lock = threading.Lock() # one sender at a time

    def label(self):
        addr = f"{self.addr[0]}:{self.addr[1]}"
        return f"{self.node_id} ({addr})" if self.node_id else addr


class Node:
    """One node: discovery, peer table, file lists, transfers."""

    def __init__(self, tcp_port, node_dir):
        # Random id per run: skip own broadcasts, name peers
        self.node_id = uuid.uuid4().hex[:8]
        self.tcp_port = tcp_port
        self.running = True

        self.shared_dir = os.path.join(node_dir, "shared")
        self.downloads_dir = os.path.join(node_dir, "downloads")
        os.makedirs(self.shared_dir, exist_ok=True)
        os.makedirs(self.downloads_dir, exist_ok=True)

        # Shared between threads, only touched while holding self.lock
        self.lock = threading.Lock()
        self.peers = {} # socket -> Peer (peer table)
        self.connecting = set() # node_ids being dialed right now
        self.pending = {} # filename -> peer it was requested from

        self.udp_sock = self._make_udp_socket()
        self.tcp_sock = self._make_tcp_socket()

    # ---------- startup / shutdown ----------

    def start(self):
        # Listeners start before the broadcast so no ack is missed
        for target in (self.udp_listener, self.accept_loop, self.file_list_loop):
            threading.Thread(target=target, daemon=True).start()
        self.broadcast_peer_request()

    def stop(self):
        """Close every socket. No goodbye message, peers see the closed socket."""
        self.running = False
        close_socket(self.udp_sock)
        close_socket(self.tcp_sock)
        with self.lock:
            peers = list(self.peers.values())
        for peer in peers:
            close_socket(peer.sock)

    # ---------- sockets ----------

    @staticmethod
    def _make_udp_socket():
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Several nodes on one machine can bind 54321 (mac needs SO_REUSEPORT)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("", p.UDP_PORT))
        return sock

    def _make_tcp_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", self.tcp_port))
        sock.listen()
        return sock

    # ---------- UDP discovery ----------

    def broadcast_peer_request(self):
        p.send_udp(self.udp_sock, p.make_peer_request(self.node_id), BROADCAST_ADDR)

    def udp_listener(self):
        # Runs for the node's lifetime, so nodes that join later are found.
        # Acks are broadcast with "to" = requester: with several nodes on
        # one machine, a unicast to port 54321 reaches only one of them.
        while self.running:
            try:
                msg, addr = p.recv_udp(self.udp_sock, own_id=self.node_id)
                if msg is None:
                    continue

                if msg["type"] == p.PEER_REQUEST:
                    ack = p.make_peer_ack(self.node_id, self.tcp_port, msg["node_id"])
                    p.send_udp(self.udp_sock, ack, BROADCAST_ADDR)

                elif msg["type"] == p.PEER_ACK and msg["to"] == self.node_id:
                    # New node is the TCP client. Own thread so a slow
                    # connect doesn't block UDP.
                    args = (addr[0], msg["tcp_port"], msg["node_id"])
                    threading.Thread(target=self.connect_to_peer, args=args,
                                     daemon=True).start()
            except OSError:
                if not self.running:
                    break # socket closed on quit

    # ---------- TCP connections ----------

    def connect_to_peer(self, ip, port, peer_id):
        # Never open a second connection to the same node
        with self.lock:
            if peer_id in self.connecting or self._find_peer(peer_id):
                return
            self.connecting.add(peer_id)

        try:
            sock = socket.create_connection((ip, port), timeout=CONNECT_TIMEOUT)
            sock.settimeout(None)
        except (OSError, TypeError) as e:
            with self.lock:
                self.connecting.discard(peer_id)
            print(f"[peer] could not connect to {ip}:{port}: {e}")
            return

        self.add_peer(sock, (ip, port), outbound=True, node_id=peer_id)

    def accept_loop(self):
        # Ack sender is the TCP server
        while self.running:
            try:
                conn, addr = self.tcp_sock.accept()
            except OSError:
                break # socket closed on quit
            self.add_peer(conn, addr, outbound=False)

    def add_peer(self, sock, addr, outbound, node_id=None):
        peer = Peer(sock, addr, outbound, node_id)
        with self.lock:
            self.peers[sock] = peer
            self.connecting.discard(node_id)

        p.log_peer_change("PEER ADDED", addr)
        print(f"[peer] added {peer.label()}")

        # File list right after a new connection
        self.send_file_list(peer)
        threading.Thread(target=self.recv_loop, args=(peer,), daemon=True).start()

    def remove_peer(self, peer, action="PEER REMOVED"):
        """Drop peer from the table (its file list goes with it) and close the socket."""
        with self.lock:
            # Can be called from several threads, only the first one removes
            removed = self.peers.get(peer.sock) is peer
            if removed:
                del self.peers[peer.sock]
            # Requests sent to this peer will never be answered
            lost = [name for name, pr in self.pending.items() if pr is peer]
            for name in lost:
                del self.pending[name]

        close_socket(peer.sock)

        if self.running:
            if removed:
                p.log_peer_change(action, peer.addr)
                print(f"[peer] removed {peer.label()}")
            for name in lost:
                print(f"[get] {name} cancelled, {peer.label()} left")

    def _find_peer(self, node_id, exclude=None):
        """Peer with this node_id, or None. Caller holds self.lock."""
        for peer in self.peers.values():
            if peer.node_id == node_id and peer is not exclude:
                return peer
        return None

    def send(self, peer, msg):
        """Send msg to peer, drop the peer if the socket is dead."""
        try:
            p.send_msg(peer.sock, msg, peer.addr, peer.send_lock)
            return True
        except OSError:
            self.remove_peer(peer)
            return False

    # ---------- receive loop (one per peer) ----------

    def recv_loop(self, peer):
        while True:
            try:
                msg = p.recv_msg(peer.sock, peer.addr)
            except p.ProtocolError as e:
                print(f"[peer] bad message from {peer.label()}: {e}")
                msg = None

            # None = closed, reset, or died mid-message
            if msg is None:
                break
            self.handle_message(peer, msg)

        self.remove_peer(peer)

    def handle_message(self, peer, msg):
        handlers = {
            p.FILE_LIST: self.on_file_list,
            p.DATA_REQUEST: self.on_data_request,
            p.FILE_DATA: self.on_file_data,
            p.ERROR: self.on_error,
        }
        handler = handlers.get(msg["type"])
        if handler:
            handler(peer, msg)
        # peer_request / peer_ack over TCP: ignore

    # ---------- file lists ----------

    def list_shared(self):
        """Names of files in shared/, hidden temp files skipped."""
        return sorted(
            name for name in os.listdir(self.shared_dir)
            if not name.startswith(".")
            and os.path.isfile(os.path.join(self.shared_dir, name))
        )

    def send_file_list(self, peer):
        self.send(peer, p.make_file_list(self.node_id, self.list_shared()))

    def file_list_loop(self):
        while self.running:
            time.sleep(FILE_LIST_INTERVAL)
            msg = p.make_file_list(self.node_id, self.list_shared())

            # Send outside the lock so a slow peer doesn't block other threads
            with self.lock:
                peers = list(self.peers.values())
            for peer in peers:
                self.send(peer, msg)

    def on_file_list(self, peer, msg):
        files = msg["files"] if isinstance(msg["files"], list) else []
        with self.lock:
            peer.node_id = str(msg["node_id"])
            peer.files = [name for name in files if isinstance(name, str)]
            other = self._find_peer(peer.node_id, exclude=peer)

        if other:
            self.drop_duplicate(peer, other)

    def drop_duplicate(self, peer, other):
        # Two nodes starting at the same time can both dial each other.
        # Both ends keep the connection dialed by the smaller node_id.
        keep_outbound = self.node_id < peer.node_id
        extra = other if peer.outbound == keep_outbound else peer
        self.remove_peer(extra, action="DUPLICATE CLOSED")

    # ---------- serving files ----------

    def on_data_request(self, peer, msg):
        # Own thread so this receive loop keeps reading. Two nodes sending
        # big files to each other from their receive loops would deadlock.
        threading.Thread(target=self.serve_file, args=(peer, msg["filename"]),
                         daemon=True).start()

    def serve_file(self, peer, filename):
        # Missing file gets an error reply, the connection stays open
        reply = p.make_error(filename, "file not found")

        path = self._shared_path(filename)
        if path and os.path.isfile(path):
            try:
                with open(path, "rb") as f:
                    reply = p.make_file_data(filename, f.read())
            except OSError:
                pass # deleted or unreadable, keep the error reply

        try:
            self.send(peer, reply)
        except p.ProtocolError:
            self.send(peer, p.make_error(filename, "file too large"))

    def _shared_path(self, filename):
        """Path inside shared/, or None for names like '../x' or '.hidden'."""
        if not isinstance(filename, str) or not filename:
            return None
        if filename != os.path.basename(filename) or filename.startswith("."):
            return None
        return os.path.join(self.shared_dir, filename)

    # ---------- downloading files ----------

    def on_file_data(self, peer, msg):
        filename = msg["filename"]
        with self.lock:
            # Only accept a requested file, from the peer it was requested from
            if not isinstance(filename, str) or self.pending.get(filename) is not peer:
                return
            del self.pending[filename]

        try:
            data = p.decode_file_content(msg)
            self.save_download(filename, data)
        except (p.ProtocolError, OSError) as e:
            print(f"[get] {filename} from {peer.label()} failed: {e}")
            return

        print(f"[get] {filename} downloaded from {peer.label()} ({len(data)} bytes)")

    def save_download(self, filename, data):
        # downloads/ first, shared/ only once the file is complete
        download_path = os.path.join(self.downloads_dir, filename)
        with open(download_path, "wb") as f:
            f.write(data)

        shared_path = os.path.join(self.shared_dir, filename)
        if os.path.exists(shared_path):
            return # same name already shared, keep that copy

        # Hidden temp name, then atomic rename: a file list never shows
        # a half-copied file
        tmp_path = os.path.join(self.shared_dir, f".{filename}.part")
        shutil.copyfile(download_path, tmp_path)
        os.replace(tmp_path, shared_path)

    def on_error(self, peer, msg):
        name = str(msg["filename"])
        with self.lock:
            if self.pending.get(name) is peer:
                del self.pending[name]
        print(f"[get] {peer.label()} could not send {name}: {msg['reason']}")

    # ---------- user commands ----------

    def cmd_list(self):
        with self.lock:
            rows = [(peer.label(), list(peer.files)) for peer in self.peers.values()]

        if not rows:
            print("no peers")
            return
        for label, files in sorted(rows):
            print(f"  {label}: {', '.join(files) or '(no files)'}")

    def cmd_files(self):
        print("  " + (", ".join(self.list_shared()) or "(shared/ is empty)"))

    def cmd_get(self, filename):
        shared_path = self._shared_path(filename)
        if shared_path is None:
            print("bad filename")
            return
        # Files are identified by name only, never replace one already shared
        if os.path.exists(shared_path):
            print(f"{filename} is already in shared/")
            return

        with self.lock:
            waiting = filename in self.pending
            peer = None
            if not waiting:
                peer = next((pr for pr in self.peers.values() if filename in pr.files), None)
                if peer:
                    self.pending[filename] = peer

        if waiting:
            print(f"already waiting for {filename}")
            return
        if peer is None:
            print(f"no peer has {filename}, check 'list'")
            return

        print(f"requesting {filename} from {peer.label()}")
        self.send(peer, p.make_data_request(filename))


def command_loop(node):
    print("commands: list | files | get <filename> | quit")
    while True:
        try:
            line = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        parts = line.split(maxsplit=1)
        if not parts:
            continue
        cmd = parts[0]

        if cmd == "quit":
            break
        elif cmd == "list":
            node.cmd_list()
        elif cmd == "files":
            node.cmd_files()
        elif cmd == "get" and len(parts) == 2:
            node.cmd_get(parts[1])
        else:
            print("commands: list | files | get <filename> | quit")


def main():
    if len(sys.argv) not in (2, 3) or not sys.argv[1].isdigit():
        print("Usage: python3 agent.py <tcp_port> [node_dir]")
        sys.exit(1)

    tcp_port = int(sys.argv[1])
    node_dir = sys.argv[2] if len(sys.argv) == 3 else "."
    os.makedirs(node_dir, exist_ok=True)

    p.setup_logging(os.path.join(node_dir, "log.txt"))

    try:
        node = Node(tcp_port, node_dir)
    except OSError as e:
        print(f"could not open sockets: {e}")
        sys.exit(1)

    node.start()
    print(f"node {node.node_id} on TCP port {tcp_port}, folder {node_dir}")

    command_loop(node)
    node.stop()


if __name__ == "__main__":
    main()