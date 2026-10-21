# PA3: P2P File Sharing Agent

Peer-to-peer file sharing. Every node is both server and client.
UDP broadcast to find peers, TCP for everything after that.

## Files

- `agent.py`: the node (threads, peer table, file lists, transfers, commands)
- `protocol.py`: message format (build, frame, validate, log)
- `shared/`: sample files
- `node1/` to `node5/`: `shared/` and `log.txt` from the demo run
- `log.txt` lines: timestamp, SENT/RECEIVED, message type, peer address, plus PEER ADDED / PEER REMOVED

## How to run

Python 3, standard library only.

```
python3 agent.py <tcp_port> [node_dir]
```

- `tcp_port`: TCP port this node listens on
- `node_dir`: folder for this node's `shared/`, `downloads/` and `log.txt` (default `.`, created if missing)

Five nodes on one machine, one terminal each:

```
python3 agent.py 5001 node1
python3 agent.py 5002 node2
python3 agent.py 5003 node3
python3 agent.py 5004 node4
python3 agent.py 5005 node5
```

| Command | Action |
|---------|--------|
| `list` | Peers and the files each one advertises |
| `files` | Own files in `shared/` |
| `get <filename>` | Download a file from a peer |
| `quit` | Close all sockets and exit |

Run on home Wi-Fi or a hotspot. Campus Wi-Fi blocks broadcast.

## Message format

### Encoding

Every message is one JSON object, UTF-8. The `type` field names the message, so every message identifies itself.

### Message boundaries

- **UDP** (port 54321): one message per datagram, no prefix needed.
- **TCP**: 4-byte length (big-endian unsigned int), then the JSON payload.

```
[ length: 4 bytes ][ JSON payload: length bytes ]
  00 00 00 31       {"type": "data_request", "filename": "alpha.txt"}
  (0x31 = 49)
```

Receiver reads exactly 4 bytes, then exactly `length` bytes, looping on `recv` until every byte arrives.
Max message size: 50 MB.

### File content

Base64 text in the `content` field of `file_data`.

Why: the file stays inside one JSON message, so one parser and one framing rule cover every message type.
Cost: about 33% bigger, and the whole file is held in memory (max file about 37 MB).

### Message types

| Type | Sent over | Fields | Purpose |
|------|-----------|--------|---------|
| `peer_request` | UDP broadcast | `node_id` | New node looking for peers |
| `peer_ack` | UDP broadcast | `node_id`, `tcp_port`, `to` | Answer with the TCP port to connect to |
| `file_list` | TCP | `node_id`, `files` | Filenames in `shared/` |
| `data_request` | TCP | `filename` | Ask for one file |
| `file_data` | TCP | `filename`, `content` | The file, base64 |
| `error` | TCP | `filename`, `reason` | File can't be served |

| Field | Type | Meaning |
|-------|------|---------|
| `type` | string | Message type, in every message |
| `node_id` | string | Random 8-hex id, new every run |
| `tcp_port` | int | TCP port of the node sending the ack |
| `to` | string | `node_id` of the node that sent the request |
| `files` | list of strings | Filenames only |
| `filename` | string | One filename, no path |
| `content` | string | Base64 of the file bytes |
| `reason` | string | Why it failed, e.g. `"file not found"` |

```json
{"type": "peer_request", "node_id": "3f9a1c2e"}
{"type": "peer_ack", "node_id": "b71d04aa", "tcp_port": 5001, "to": "3f9a1c2e"}
{"type": "file_list", "node_id": "b71d04aa", "files": ["alpha.txt", "notes.txt"]}
{"type": "data_request", "filename": "alpha.txt"}
{"type": "file_data", "filename": "alpha.txt", "content": "aGVsbG8gZnJvbSBub2RlMQo="}
{"type": "error", "filename": "gone.txt", "reason": "file not found"}
```

Invalid message (bad JSON, unknown type, missing field): on TCP the connection is closed and the peer removed, on UDP the datagram is ignored.

### Flow

1. New node broadcasts `peer_request` on UDP 54321. Own broadcasts are ignored (same `node_id`).
2. Every other node broadcasts a `peer_ack` with its TCP port, `to` = the requester's `node_id`. Only the requester acts on it.
   Broadcast instead of unicast: nodes on one machine share port 54321, and a unicast reply reaches only one of them.
3. New node connects over TCP (new node = client, ack sender = server). One connection per pair.
4. Both sides send `file_list` right after connecting, then every 10 seconds.
5. `data_request` gets `file_data`, or `error` if the file is gone. Connection stays open either way.
6. Download is written to `downloads/` first, then copied into `shared/`. Files already in `shared/` are never requested.
7. No goodbye message. Closed socket (empty `recv` or reset) means the peer left: peer and its files removed from the table.

## Division of work

Solo work, approved by the professor. All commits by me (Mohammed Nassar).

## Execution example

Five nodes on one Mac, started node1 to node5 a few seconds apart.

| Node | node_id | Starting files |
|------|---------|----------------|
| node1 | `80073bcb` | `alpha.txt` |
| node2 | `ee60ebcf` | `beta.txt` |
| node3 | `0ce7f0bc` | `gamma.txt` |
| node4 | `7d011df7` | `delta.txt`, `notes.txt` |
| node5 | `ddcd7d6f` | none |

Peers that connected in show their client port (e.g. `60873`), so match them by `node_id`.

**node5**: discovery, file lists, file transfer

```
$ python3 agent.py 5005 node5
node ddcd7d6f on TCP port 5005, folder node5
commands: list | files | get <filename> | quit
> [peer] added 80073bcb (192.168.12.104:5001)
[peer] added 7d011df7 (192.168.12.104:5004)
[peer] added ee60ebcf (192.168.12.104:5002)
[peer] added 0ce7f0bc (192.168.12.104:5003)
list
  0ce7f0bc (192.168.12.104:5003): gamma.txt
  7d011df7 (192.168.12.104:5004): delta.txt, notes.txt
  80073bcb (192.168.12.104:5001): alpha.txt
  ee60ebcf (192.168.12.104:5002): beta.txt
> get alpha.txt
requesting alpha.txt from 80073bcb (192.168.12.104:5001)
> [get] alpha.txt downloaded from 80073bcb (192.168.12.104:5001) (17 bytes)
files
  alpha.txt
> [peer] removed 80073bcb (192.168.12.104:5001)
```

**node2**: file lists propagating (`alpha.txt` now under node5 `ddcd7d6f`), node1 leaving

```
$ python3 agent.py 5002 node2
node ee60ebcf on TCP port 5002, folder node2
commands: list | files | get <filename> | quit
> [peer] added 80073bcb (192.168.12.104:5001)
[peer] added 192.168.12.104:60873
[peer] added 192.168.12.104:60876
[peer] added 192.168.12.104:60881
list
  0ce7f0bc (192.168.12.104:60873): gamma.txt
  7d011df7 (192.168.12.104:60876): delta.txt, notes.txt
  80073bcb (192.168.12.104:5001): alpha.txt
  ddcd7d6f (192.168.12.104:60881): alpha.txt
> [peer] removed 80073bcb (192.168.12.104:5001)
list
  0ce7f0bc (192.168.12.104:60873): gamma.txt
  7d011df7 (192.168.12.104:60876): delta.txt, notes.txt
  ddcd7d6f (192.168.12.104:60881): alpha.txt
```

**node1**: leaves with `quit` (node3 and node4 also print `[peer] removed 80073bcb`)

```
$ python3 agent.py 5001 node1
node 80073bcb on TCP port 5001, folder node1
commands: list | files | get <filename> | quit
> [peer] added 192.168.12.104:60871
[peer] added 192.168.12.104:60872
[peer] added 192.168.12.104:60875
[peer] added 192.168.12.104:60878
quit
```
