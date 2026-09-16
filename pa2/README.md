# PA2: Leader Election

## Description

This program implements leader election using an asynchronous ring of processes.

Each process generates a UUID using `uuid.uuid4()`. The processes communicate over TCP sockets and forward UUIDs around the ring. The process with the highest UUID is elected as the leader. Once the leader is determined, the leader ID is sent around the ring so that every process agrees on the same leader.

## Files

* `myleprocess.py` - implementation of one process in the leader election ring
* `config.txt` - configuration file used for the in-class demo
* `config1.txt` - configuration for local demo node 1
* `config2.txt` - configuration for local demo node 2
* `config3.txt` - configuration for local demo node 3
* `log1.txt` - log produced by local demo node 1
* `log2.txt` - log produced by local demo node 2
* `log3.txt` - log produced by local demo node 3
* `README.md` - instructions for running the program

## Config Format

Each configuration file contains two lines:

```text
my_ip,my_port
neighbor_ip,neighbor_port
```

The first line is the IP address and port where the process listens as a server.

The second line is the IP address and port of the next process in the ring, which this process connects to as a client.

For example:

```text
127.0.0.1,5001
127.0.0.1,5002
```

## Local Demo

For the local three-process demo, the configuration files form this ring:

```text
5001 -> 5002 -> 5003 -> 5001
```

Open three terminals in the `pa2` directory.

### Terminal 1

```bash
python3 myleprocess.py config1.txt log1.txt
```

### Terminal 2

```bash
python3 myleprocess.py config2.txt log2.txt
```

### Terminal 3

```bash
python3 myleprocess.py config3.txt log3.txt
```

The processes may be started in any order. If a neighboring process is not running yet, the client retries the connection until it becomes available.

## Example Execution

### Terminal 1

```text
mo@Doctor-of-Philosophy pa2 % python3 myleprocess.py config1.txt log1.txt

[d4f1b7bb-4e11-4c2a-ba8d-e7a731ce9890] Listening on ('127.0.0.1', 5001) ...
[d4f1b7bb-4e11-4c2a-ba8d-e7a731ce9890] Connected to neighbor ('127.0.0.1', 5002)
[d4f1b7bb-4e11-4c2a-ba8d-e7a731ce9890] Accepted connection from ('127.0.0.1', 52558)
leader is fd8231b7-e73b-4347-ad3f-e1c1ef80458e
```

### Terminal 2

```text
mo@Doctor-of-Philosophy pa2 % python3 myleprocess.py config2.txt log2.txt

[df98389a-dacb-44c7-ab29-13864baa9c7c] Listening on ('127.0.0.1', 5002) ...
[df98389a-dacb-44c7-ab29-13864baa9c7c] Accepted connection from ('127.0.0.1', 52556)
[df98389a-dacb-44c7-ab29-13864baa9c7c] Connected to neighbor ('127.0.0.1', 5003)
leader is fd8231b7-e73b-4347-ad3f-e1c1ef80458e
```

### Terminal 3

```text
mo@Doctor-of-Philosophy pa2 % python3 myleprocess.py config3.txt log3.txt

[fd8231b7-e73b-4347-ad3f-e1c1ef80458e] Listening on ('127.0.0.1', 5003) ...
[fd8231b7-e73b-4347-ad3f-e1c1ef80458e] Connected to neighbor ('127.0.0.1', 5001)
[fd8231b7-e73b-4347-ad3f-e1c1ef80458e] Accepted connection from ('127.0.0.1', 52560)
leader is fd8231b7-e73b-4347-ad3f-e1c1ef80458e
```

All three processes elected the same UUID:

```text
fd8231b7-e73b-4347-ad3f-e1c1ef80458e
```

The three generated log files contain the messages sent, received, forwarded, and ignored during the election.

