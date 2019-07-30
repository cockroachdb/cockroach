import socket
import sys

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(("0.0.0.0", 26257))
server.listen(1)
print "ready"
client_socket, addr = server.accept()
print "connected"

while True:
    c = client_socket.recv(1)
    if c:
        sys.stdout.write("%c" % c)
        sys.stdout.flush()
