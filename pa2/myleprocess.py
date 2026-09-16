import socket
import threading
import json
import uuid
import sys
import time


# Represents a message passed between nodes in the ring.
class Message:
    def __init__(self, uuid_value, flag):
        # Store the process ID as a UUID object.
        self.uuid = uuid.UUID(str(uuid_value))

        # flag = 0 means leader election is still in progress.
        # flag = 1 means a leader has already been elected.
        self.flag = flag

    def to_json(self):
        # UUID objects are converted to strings before being serialized.
        return json.dumps({
            "uuid": str(self.uuid),
            "flag": self.flag
        })

    @staticmethod
    def from_json(data):
        # Convert the received JSON string back into a Message object.
        obj = json.loads(data)
        return Message(obj["uuid"], obj["flag"])


class LeaderElectionNode:
    def __init__(self, config_path, log_path):
        # Generate one unique ID for this process.
        self.my_id = uuid.uuid4()

        # leader_id stays None until a leader is discovered.
        self.leader_id = None

        self.log_path = log_path
        self.log_lock = threading.Lock()

        # Start a new log file and record this process's ID first.
        with open(self.log_path, "w") as f:
            f.write(f"My id: {self.my_id}\n")

        # Read this node's address and its outgoing neighbor's address.
        self.my_addr, self.neighbor_addr = self._read_config(config_path)

        # conn_in receives messages from the previous node.
        # conn_out sends messages to the next node.
        self.server_socket = None
        self.conn_in = None
        self.conn_out = None

        # Stores extra bytes if more than one message is received at once.
        self.recv_buf = b""

        # Controls the main receive loop.
        self.running = True

    def _read_config(self, path):
        # Read the two non-empty lines from the configuration file.
        with open(path, "r") as f:
            lines = [line.strip() for line in f if line.strip()]

        # First line is this process's server address.
        my_ip, my_port = lines[0].split(",")

        # Second line is the neighbor this process connects to.
        neighbor_ip, neighbor_port = lines[1].split(",")

        return (
            (my_ip, int(my_port)),
            (neighbor_ip, int(neighbor_port))
        )

    def log(self, line):
        # Use a lock so different threads cannot write to the log at the same time.
        with self.log_lock:
            with open(self.log_path, "a") as f:
                f.write(line + "\n")

    def start_server(self):
        # Create the server socket used to receive messages.
        self.server_socket = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        # Allow the port to be reused after restarting the program.
        self.server_socket.setsockopt(
            socket.SOL_SOCKET,
            socket.SO_REUSEADDR,
            1
        )

        # Listen on this node's configured address.
        self.server_socket.bind(self.my_addr)
        self.server_socket.listen(1)

        print(f"[{self.my_id}] Listening on {self.my_addr} ...")

        # Wait for the previous node in the ring to connect.
        conn, addr = self.server_socket.accept()
        self.conn_in = conn

        print(f"[{self.my_id}] Accepted connection from {addr}")

    def start_client(self):
        # Keep trying until the next node's server is available.
        while self.running:
            try:
                s = socket.socket(
                    socket.AF_INET,
                    socket.SOCK_STREAM
                )

                # Connect to the next node in the ring.
                s.connect(self.neighbor_addr)

                self.conn_out = s

                print(
                    f"[{self.my_id}] Connected to neighbor "
                    f"{self.neighbor_addr}"
                )

                return

            except (ConnectionRefusedError, OSError):
                # Wait briefly before trying again.
                time.sleep(1)

    def send_message(self, msg):
        # Convert the Message object to JSON and send it to the next node.
        data = msg.to_json().encode("utf-8")
        self.conn_out.sendall(data)

        # Record every outgoing message.
        self.log(
            f"Sent: uuid={msg.uuid}, flag={msg.flag}"
        )

    def recv_message(self):
        # Start with any bytes left over from a previous receive.
        buf = self.recv_buf

        # Each JSON message ends with a closing brace.
        while b"}" not in buf:
            chunk = self.conn_in.recv(4096)

            # Stop if the connection is closed.
            if not chunk:
                return None

            buf += chunk

        # Extract exactly one JSON message.
        end_idx = buf.index(b"}") + 1
        message_bytes = buf[:end_idx]

        # Save remaining bytes for the next call.
        self.recv_buf = buf[end_idx:]

        return Message.from_json(
            message_bytes.decode("utf-8")
        )

    def _compare(self, incoming_uuid):
        # Compare the received UUID with this process's UUID.
        if incoming_uuid > self.my_id:
            return "greater"

        if incoming_uuid < self.my_id:
            return "less"

        return "same"

    def run(self):
        # accept() blocks, so the server side runs in another thread.
        # This allows this process to connect to its neighbor at the same time.
        server_thread = threading.Thread(
            target=self.start_server
        )

        server_thread.start()

        # Connect to the next node in the ring.
        self.start_client()

        # Wait until the incoming connection is also established.
        server_thread.join()

        # Start the election by sending this process's UUID once.
        self.send_message(
            Message(self.my_id, 0)
        )

        # After connections are established, this process waits for messages,
        # compares UUIDs, and either forwards or ignores them.
        while self.running:
            msg = self.recv_message()

            if msg is None:
                break

            incoming_uuid = msg.uuid
            comparison = self._compare(incoming_uuid)

            # flag = 1 means the leader has already been elected.
            if msg.flag == 1:
                self.leader_id = incoming_uuid

                self.log(
                    f"Received: uuid={msg.uuid}, flag=1, "
                    f"{comparison}, state=1, "
                    f"leader={self.leader_id}"
                )

                # If the leader receives its own announcement again,
                # the announcement has completed the entire ring.
                if incoming_uuid == self.my_id:
                    print(f"leader is {self.leader_id}")

                    self.log(
                        f"leader is decided to "
                        f"{self.leader_id}."
                    )

                    self.running = False

                else:
                    # Forward the leader announcement to the next node.
                    self.send_message(
                        Message(msg.uuid, 1)
                    )

                    print(f"leader is {self.leader_id}")

                    self.log(
                        f"leader is decided to "
                        f"{self.leader_id}."
                    )

                    self.running = False

            # If this process receives its own UUID again,
            # it has the largest UUID and becomes the leader.
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

                # Announce the elected leader around the ring.
                self.send_message(
                    Message(self.my_id, 1)
                )

            # Forward UUIDs that are greater than this process's UUID.
            elif incoming_uuid > self.my_id:
                self.log(
                    f"Received: uuid={msg.uuid}, flag=0, "
                    f"{comparison}, state=0"
                )

                self.send_message(
                    Message(msg.uuid, 0)
                )

            # Ignore UUIDs smaller than this process's UUID.
            else:
                self.log(
                    f"Received: uuid={msg.uuid}, flag=0, "
                    f"{comparison}, state=0 -- Ignored"
                )


def main():
    # The program expects a config file and a log file.
    if len(sys.argv) != 3:
        print(
            "Usage: python myleprocess.py "
            "<config_file> <log_file>"
        )
        sys.exit(1)

    config_path = sys.argv[1]
    log_path = sys.argv[2]

    # Create and run one node in the leader election ring.
    node = LeaderElectionNode(
        config_path,
        log_path
    )

    node.run()


if __name__ == "__main__":
    main()