import threading
import socket
import yaml
from pathlib import Path
import json 

class Node:
    def __init__(self) -> None:
        self.node = []
        self.lock = threading.Lock()

    def __str__(self) -> str:
        with self.lock:
            return json.dumps(self.node, indent=4)

    def add(self, node):
        with self.lock:
            self.node.append(node)
    
    def get_task_node_ip(self, task):
        with self.lock:
            ret = []
            for node in self.node:
                name = next(iter(node))
                if task in node[name]["task"]:
                    ret.append(node[name]["ip"])
            return ret
        
    def get_task_list(self):
        with self.lock:
            ret = set()
            for node in self.node:
                name = next(iter(node))
                ret.add(node[name]["task"])
            return ret

class RoundRobin:
    def __init__(self, node_list: Node) -> None:
        self.task_to_ip = {}
        self.last_task_ip = {}

        for task in node_list.get_task_list():
            self.task_to_ip[task] = []
            for ip in node_list.get_task_node_ip(task):
                self.task_to_ip[task].append(ip)
            self.last_task_ip[task] = 0

        self.lock = threading.Lock()

    def get_ip(self, task):
        with self.lock:
            ip = self.task_to_ip[task][self.last_task_ip[task]]
            self.last_task_ip[task] = (self.last_task_ip[task] + 1) % len(self.task_to_ip[task])
            return ip

def load_config():
    # load yaml
    config_path = Path("config.yaml")
    if not config_path.exists():
        print("config.yaml not found.") 
        return
    with open('config.yaml') as file:
        config = yaml.full_load(file)
        for node in config["nodes"]:
            NODE.add(node)

def handle_request(data):
    pass
    

def server():
    # Listen for incoming headers and send to respective nodes or stores in queue
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', 8000))    # later change to master node IP as per yaml file
    server.listen(5)

    while True:
        conn, addr = server.accept()
        data = conn.recv(1024)
        handle_request(data)
        conn.close()

def heartbeat():
    pass


def main():
    global NODE, RR
    NODE = Node()
    load_config()
    RR = RoundRobin(NODE)
    
    server_thread = threading.Thread(target=server)
    heartbeat_thread = threading.Thread(target=heartbeat)

    server_thread.start()
    heartbeat_thread.start()

    server_thread.join()
    heartbeat_thread.join()


# Global variables
NODE = None
RR = None

if __name__ == "__main__":
    main()