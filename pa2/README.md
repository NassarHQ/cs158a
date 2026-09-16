# PA2: Leader Election

## What This Does
Each node picks a random UUID and connects to two neighbors over TCP to form a ring.
Nodes exchange messages to elect the one with the highest UUID as leader.
All nodes agree on the same leader and then stop.

## Files
myleprocess.py runs one node in the ring
config1.txt, config2.txt, config3.txt are configs for the three local demo nodes
log1.txt, log2.txt, log3.txt are the logs from the demo run

## Config Format
Each config file has two lines.
Line 1 is this node's own address (it listens here as a server).
Line 2 is the neighbor's address (it connects out to this as a client).

Example:
127.0.0.1,5001
127.0.0.1,5002

## How to Run
Open three terminals in this folder and run:
python3 myleprocess.py config1.txt log1.txt
python3 myleprocess.py config2.txt log2.txt
python3 myleprocess.py config3.txt log3.txt

Start them within a few seconds of each other.

## Example Output
mo@Doctor-of-Philosophy pa2 % python3 myleprocess.py config1.txt log1.txt
[9f2a00c3-97f7-4b46-a5ac-0f6516db8971] Listening on ('127.0.0.1', 5001) ...
[9f2a00c3-97f7-4b46-a5ac-0f6516db8971] Connected to neighbor ('127.0.0.1', 5002)
[9f2a00c3-97f7-4b46-a5ac-0f6516db8971] Accepted connection from ('127.0.0.1', 51960)
leader is 9f2a00c3-97f7-4b46-a5ac-0f6516db8971

All three nodes printed the same leader UUID, so the election worked correctly.
