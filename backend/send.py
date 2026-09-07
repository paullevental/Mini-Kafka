import struct, sys, socket


if __name__ == "__main__":
    payload = sys.argv[1].encode()

    frame = struct.pack('>ihhi', 8 + len(payload), 0, 0, 0) + payload

    with (socket.create_connection(('localhost', 8080))) as sock:
        sock.sendall(frame)

