import threading
import socket
import heart_beat
from asyncio import run

def handle_request(data):
    # Parse data and send to respective node
    pass

def server():
    # Listen for incoming headers and send to respective nodes or stores in queue
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', 5000))    # later change to master node IP as per yaml file
    server.listen(5)

    while True:
        conn, addr = server.accept()
        data = conn.recv(1024)
        handle_request(data)
        conn.close()

def heartbeat():
    responsible_node_url = "http://127.0.0.1:5000/report"
    node_urls = [
        "https://github.com/iiteen",
        "https://github.com/wadetb/heartbeat",
        "http://127.0.0.1:80/",
        # Add more node URLs as needed
    ]

    run(heart_beat.main(node_urls, responsible_node_url))


def main():
    server_thread = threading.Thread(target=server)
    heartbeat_thread = threading.Thread(target=heartbeat)

    server_thread.start()
    heartbeat_thread.start()

    server_thread.join()
    heartbeat_thread.join()

if __name__ == "__main__":
    main()