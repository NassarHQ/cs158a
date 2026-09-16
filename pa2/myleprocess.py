import socket
import threading
import json
import uuid
import sys
import time


class Message:
    def __init__(self, uuid_value, flag):
        self.uuid = uuid.UUID(str(uuid_value))
        self.flag = flag

    def to_json(self):
        return json.dumps({
            "uuid": str(self.uuid),
            "flag": self.flag
        })

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
        self.recv_buf = b""

        self.running = True

    def _read_config(self, path):
        with open(path, "r") as f:
            lines = [line.strip() for line in f if line.strip()]

        my_ip, my_port = lines[0].split(",")
        neighbor_ip, neighbor_port = lines[1].split(",")

        return (
            (my_ip, int(my_port)),
            (neighbor_ip, int(neighbor_port))
        )

    def log(self, line):
        with self.log_lock:
            with open(self.log_path, "a") as f:
                f.write(line + "\n")

    def start_server(self):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        self.server_socket.bind(self.my_addr)
        self.server_socket.listen(1)

        print(f"[{self.my_id}] Listening on {self.my_addr} ...")

        conn, addr = self.server_socket.accept()
        self.conn_in = conn

        print(f"[{self.my_id}] Accepted connection from {addr}")

    def start_client(self):
        while self.running:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(self.neighbor_addr)

                self.conn_out = s

                print(
                    f"[{self.my_id}] Connected to neighbor "
                    f"{self.neighbor_addr}"
                )
                return

            except (ConnectionRefusedError, OSError):
                time.sleep(1)

    def send_message(self, msg):
        data = msg.to_json().encode("utf-8")
        self.conn_out.sendall(data)

        self.log(
            f"Sent: uuid={msg.uuid}, flag={msg.flag}"
        )

    def recv_message(self):
        buf = self.recv_buf

        while b"}" not in buf:
            chunk = self.conn_in.recv(4096)

            if not chunk:
                return None

            buf += chunk

        end_idx = buf.index(b"}") + 1
        message_bytes = buf[:end_idx]
        self.recv_buf = buf[end_idx:]

        return Message.from_json(
            message_bytes.decode("utf-8")
        )

    def _compare(self, incoming_uuid):
        if incoming_uuid > self.my_id:
            return "greater"

        if incoming_uuid < self.my_id:
            return "less"

        return "same"

    def run(self):
        server_thread = threading.Thread(
            target=self.start_server
        )

        server_thread.start()
        self.start_client()
        server_thread.join()

        # Send this node's UUID once when the election starts.
        self.send_message(
            Message(self.my_id, 0)
        )

        while self.running:
            msg = self.recv_message()

            if msg is None:
                break

            incoming_uuid = msg.uuid
            comparison = self._compare(incoming_uuid)

            if msg.flag == 1:
                self.leader_id = incoming_uuid

                self.log(
                    f"Received: uuid={msg.uuid}, flag=1, "
                    f"{comparison}, state=1, "
                    f"leader={self.leader_id}"
                )

                if incoming_uuid == self.my_id:
                    print(f"leader is {self.leader_id}")
                    self.log(
                        f"leader is decided to "
                        f"{self.leader_id}."
                    )
                    self.running = False

                else:
                    self.send_message(
                        Message(msg.uuid, 1)
                    )

                    print(f"leader is {self.leader_id}")
                    self.log(
                        f"leader is decided to "
                        f"{self.leader_id}."
                    )
                    self.running = False

            elif incoming_uuid == self.my_id:
                self.log(
                    f"Received: uuid={msg.uuid}, flag=0, "
                    f"{comparison}, state=0"
                )

                self.leader_id = self.my_id

                self.log(
                    f"leader is decided to "
                    f"{self.leader_id}."
                )

                self.send_message(
                    Message(self.my_id, 1)
                )

            elif incoming_uuid > self.my_id:
                self.log(
                    f"Received: uuid={msg.uuid}, flag=0, "
                    f"{comparison}, state=0"
                )

                self.send_message(
                    Message(msg.uuid, 0)
                )

            else:
                self.log(
                    f"Received: uuid={msg.uuid}, flag=0, "
                    f"{comparison}, state=0 -- Ignored"
                )


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: python myleprocess.py "
            "<config_file> <log_file>"
        )
        sys.exit(1)

    config_path = sys.argv[1]
    log_path = sys.argv[2]

    node = LeaderElectionNode(
        config_path,
        log_path
    )

    node.run()


if __name__ == "__main__":
    main()
