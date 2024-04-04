import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('localhost', 10000))
sock.sendall("wholesome sex".encode("utf-8"))
response = sock.recv(1024)
print(response.decode("utf-8"))
sock.close()