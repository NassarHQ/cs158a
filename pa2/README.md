# PA2: Leader Election

## What This Does
Each node picks a random UUID and connects to two neighbors over TCP to form a ring.
Nodes exchange messages to elect the one with the highest UUID as leader.
All nodes agree on the same leader and then stop.

## Files
myleprocess.py runs one node in the ring
config.txt is a generic example config
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
Open three separate terminals in this folder and run one command in each,
at the same time:
python3 myleprocess.py config1.txt log1.txt
python3 myleprocess.py config2.txt log2.txt
python3 myleprocess.py config3.txt log3.txt

All three must be running at once since each one blocks until the ring
completes. Start them within a few seconds of each other.

## Example Output
mo@Doctor-of-Philosophy pa2 % python3 myleprocess.py config1.txt log1.txt
[ef8ccaa0-4c2c-448d-a731-fc71810d2f04] Listening on ('127.0.0.1', 5001) ...
[ef8ccaa0-4c2c-448d-a731-fc71810d2f04] Connected to neighbor ('127.0.0.1', 5002)
[ef8ccaa0-4c2c-448d-a731-fc71810d2f04] Accepted connection from ('127.0.0.1', 53554)
leader is ef8ccaa0-4c2c-448d-a731-fc71810d2f04

mo@Doctor-of-Philosophy pa2 % python3 myleprocess.py config2.txt log2.txt
[d4fce45d-1254-4918-a8af-8869d113473e] Listening on ('127.0.0.1', 5002) ...
[d4fce45d-1254-4918-a8af-8869d113473e] Accepted connection from ('127.0.0.1', 53549)
[d4fce45d-1254-4918-a8af-8869d113473e] Connected to neighbor ('127.0.0.1', 5003)
leader is ef8ccaa0-4c2c-448d-a731-fc71810d2f04

mo@Doctor-of-Philosophy pa2 % python3 myleprocess.py config3.txt log3.txt
[c51b5084-f15f-4f1b-81e7-4475689d18a4] Listening on ('127.0.0.1', 5003) ...
[c51b5084-f15f-4f1b-81e7-4475689d18a4] Connected to neighbor ('127.0.0.1', 5001)
[c51b5084-f15f-4f1b-81e7-4475689d18a4] Accepted connection from ('127.0.0.1', 53557)
leader is ef8ccaa0-4c2c-448d-a731-fc71810d2f04

All three nodes agreed on the same leader UUID, confirming Termination,
Uniqueness, and Agreement.
