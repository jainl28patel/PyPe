import socket
import threading
import json
import time
import random


def handle_client(conn):
    while True:
        data = conn.recv(1024)
        if not data:
            break
        request = json.loads(data.decode())
        print(f"Received {request} from client")
        print(f"Processing task {request['task']} with id {request['task_id']}")
        time.sleep(random.randint(1, 10))
        # Process the task and determine the status
        status = "yes"  # or "yes" based on the task processing result
        print(f"Send: {status}")
        conn.sendall(json.dumps({"status": status}).encode())
    conn.close()


def node_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("localhost", 8080))
        s.listen()
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client, args=(conn,)).start()


# Run the node server
node_server()
