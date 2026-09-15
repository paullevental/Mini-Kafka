"""Open as many concurrent connections to the broker as possible.

Connects and sends nothing, so each server thread parks in readInt() and the
connection stays open. Measures where the broker's thread model gives out.

    python3 bench_connections.py 10000
"""
import socket
import sys

TARGET = int(sys.argv[1]) if len(sys.argv) > 1 else 10000
HOST, PORT = "localhost", 8080

socks = []
try:
    for i in range(TARGET):
        s = socket.create_connection((HOST, PORT))
        socks.append(s)
        if (i + 1) % 500 == 0:
            print(f"  {i + 1} open", flush=True)
except (OSError, Exception) as e:
    print(f"\nFAILED at {len(socks)} connections")
    print(f"  {type(e).__name__}: {e}")
else:
    print(f"\nheld {len(socks)} concurrent connections")

print("\nleaving them open — measure the broker now, then press Enter")
print("  ps -o rss= -p $(pgrep -f com.minikafka.Broker)   # RSS in KB")
input()

for s in socks:
    s.close()
