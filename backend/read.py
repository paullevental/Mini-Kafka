import struct, sys, socket


if __name__ == "__main__":

    frame = struct.pack('>ihhi', 8,1,0,0)

    with (socket.create_connection(('localhost', 8080))) as sock:
        sock.sendall(frame)
        f = sock.makefile('rb')
        N = struct.unpack('>i', f.read(4))[0]
        data = f.read(N)
        print(data.decode())