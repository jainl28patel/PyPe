import socket
import json

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("localhost", 8000))
# send json data
data = {
    "task": "t1",
    "task_id": "1212",
}

sock.sendall(json.dumps(data).encode())
sock.close()