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
[8059a780-80b6-49a8-8fcf-979dc069fb2c] Listening on ('127.0.0.1', 5001) ...
[8059a780-80b6-49a8-8fcf-979dc069fb2c] Connected to neighbor ('127.0.0.1', 5002)
[8059a780-80b6-49a8-8fcf-979dc069fb2c] Accepted connection from ('127.0.0.1', 53099)
leader is 8059a780-80b6-49a8-8fcf-979dc069fb2c

mo@Doctor-of-Philosophy pa2 % python3 myleprocess.py config2.txt log2.txt
[7b26bef2-c11c-49cd-b984-4a640d370f35] Listening on ('127.0.0.1', 5002) ...
[7b26bef2-c11c-49cd-b984-4a640d370f35] Accepted connection from ('127.0.0.1', 53093)
[7b26bef2-c11c-49cd-b984-4a640d370f35] Connected to neighbor ('127.0.0.1', 5003)
leader is 8059a780-80b6-49a8-8fcf-979dc069fb2c

mo@Doctor-of-Philosophy pa2 % python3 myleprocess.py config3.txt log3.txt
[467d7609-546a-4a54-a965-56e1f653c97f] Listening on ('127.0.0.1', 5003) ...
[467d7609-546a-4a54-a965-56e1f653c97f] Connected to neighbor ('127.0.0.1', 5001)
[467d7609-546a-4a54-a965-56e1f653c97f] Accepted connection from ('127.0.0.1', 53100)
leader is 8059a780-80b6-49a8-8fcf-979dc069fb2c

All three nodes agreed on the same leader UUID, confirming Termination,
Uniqueness, and Agreement.
