"""
myleprocess.py

One node in a Chang-Roberts leader election ring.
Run with: python myleprocess.py <config_file> <log_file>

config file has 2 lines:
    my_ip,my_port       -> address this node listens on
    neighbor_ip,neighbor_port  -> node this connects out to
"""

import socket
import threading
import json
import uuid
import sys
import time


class Message:
    """Message exchanged between neighboring nodes during leader election."""

    def __init__(self, uuid_value, flag):
        self.uuid = str(uuid_value)
        self.flag = flag

    def to_json(self):
        return json.dumps({"uuid": self.uuid, "flag": self.flag})

    @staticmethod
    def from_json(data):
        obj = json.loads(data)
        return Message(obj["uuid"], obj["flag"])


class LeaderElectionNode:
    def __init__(self, config_path, log_path):
        self.my_id = uuid.uuid4()
        self.leader_id = None
        self.log_path = log_path
        self.log_lock = threading.Lock()

        with open(self.log_path, "w") as f:
            f.write(f"My id: {self.my_id}\n")

        self.my_addr, self.neighbor_addr = self._read_config(config_path)

        self.server_socket = None
        self.conn_in = None
        self.conn_out = None
        self.recv_buf = b""  # leftover bytes after the last '}' delimiter

        self.running = True

    def _read_config(self, path):
        with open(path, "r") as f:
            lines = [line.strip() for line in f if line.strip()]
        my_ip, my_port = lines[0].split(",")
        nb_ip, nb_port = lines[1].split(",")
        return (my_ip, int(my_port)), (nb_ip, int(nb_port))

    def log(self, line):
        with self.log_lock:
            with open(self.log_path, "a") as f:
                f.write(line + "\n")

    # ---------- networking setup ----------

    def start_server(self):
        """Bind and listen; accept exactly one connection from upstream neighbor."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(self.my_addr)
        self.server_socket.listen(1)
        print(f"[{self.my_id}] Listening on {self.my_addr} ...")
        conn, addr = self.server_socket.accept()
        self.conn_in = conn
        print(f"[{self.my_id}] Accepted connection from {addr}")

    def start_client(self):
        """Connect to downstream neighbor, retrying until it is up."""
        while self.running:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(self.neighbor_addr)
                self.conn_out = s
                print(f"[{self.my_id}] Connected to neighbor {self.neighbor_addr}")
                return
            except (ConnectionRefusedError, OSError):
                time.sleep(1)

    # ---------- message send/receive ----------

    def send_message(self, msg):
        # JSON objects always end with '}'
        data = msg.to_json().encode("utf-8")
        self.conn_out.sendall(data)
        self.log(f"Sent: uuid={msg.uuid}, flag={msg.flag}")

    def recv_message(self):
        """Read one JSON message from conn_in, delimited by a closing '}'."""
        buf = self.recv_buf
        while b"}" not in buf:
            chunk = self.conn_in.recv(4096)
            if not chunk:
                return None
            buf += chunk

        end_idx = buf.index(b"}") + 1
        message_bytes = buf[:end_idx]
        self.recv_buf = buf[end_idx:]
        return Message.from_json(message_bytes.decode("utf-8"))

    def _compare(self, incoming_uuid):
        """Returns 'greater', 'less', or 'same' relative to my own UUID."""
        if incoming_uuid > self.my_id:
            return "greater"
        elif incoming_uuid < self.my_id:
            return "less"
        else:
            return "same"

    # ---------- main algorithm ----------

    def run(self):
        # Start server thread (accept) and client connection concurrently
        # to avoid deadlock (accept() blocks; we need connect() to happen too).
        server_thread = threading.Thread(target=self.start_server)
        server_thread.start()
        self.start_client()
        server_thread.join()

        # Initial message: send our own UUID once, no comparison, flag=0
        initial_msg = Message(self.my_id, 0)
        self.send_message(initial_msg)

        # Loop while self.running:
        while self.running:
            msg = self.recv_message()

            if msg is None:
                break

            incoming_uuid = uuid.UUID(msg.uuid)
            cmp = self._compare(incoming_uuid)

            if msg.flag == 1:
                self.leader_id = msg.uuid
                self.log(f"Received: uuid={msg.uuid}, flag=1, {cmp}, state=1, leader={self.leader_id}")

                if incoming_uuid != self.my_id:
                    self.send_message(Message(msg.uuid, 1))
                
                print(f"leader is {self.leader_id}")
                self.log(f"leader is decided to {self.leader_id}.")
                self.running = False

            elif msg.flag == 0 and incoming_uuid == self.my_id:
                cmp = self._compare(incoming_uuid)
                self.log(f"Received: uuid={msg.uuid}, flag=0, {cmp}, state=0")

                self.leader_id = str(self.my_id)
                self.log(f"leader is decided to {self.leader_id}.")
                self.send_message(Message(self.my_id, 1))

                print("leader is " + self.leader_id)
                
                self.running = False

            elif msg.flag == 0 and incoming_uuid > self.my_id:
                self.log(f"Received: uuid={msg.uuid}, flag=0, {cmp}, state=0")
                self.send_message(Message(msg.uuid, 0))
            
            else:
                self.log(f"Received: uuid={msg.uuid}, flag=0, {cmp}, state=0 -- Ignored")

def main():
    if len(sys.argv) != 3:
        print("Usage: python myleprocess.py <config_file> <log_file>")
        sys.exit(1)

    config_path = sys.argv[1]
    log_path = sys.argv[2]

    node = LeaderElectionNode(config_path, log_path)
    node.run()


if __name__ == "__main__":
    main()