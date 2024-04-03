import threading
import socket
from time import sleep
from asyncio import run
import time
import yaml
from pathlib import Path
import json 
from TaskQueue import TaskQueue
import asyncio
import aiohttp
import json
import threading

class Node_Health:
    def __init__ (self, node_urls):
        self.node_urls = {}
        self.last_heartbeat_time = {}
        for url in node_urls:
            self.node_urls[url] = "down"
            self.last_heartbeat_time[url] = 0
        self.lock = threading.Lock()

    def __str__(self):
        with self.lock:
            return json.dumps(self.node_urls, indent=4)
        
    def update_health(self, url, status):
        with self.lock:
            self.node_urls[url] = status

    def get_data(self):
        with self.lock:
            return self.node_urls.copy()

# Main function to orchestrate the heartbeat checks
def heartbeat_main(node_urls):
    global NODE_HEALTH
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('10.61.8.75', 9000))
    sock.listen(20)

    while True:
        conn, addr = sock.accept()
        data = conn.recv(1024).decode()
        data = json.loads(data)
        host, port = conn.getpeername()
        if data["status"] == "up":
            NODE_HEALTH.update_health(addr[0], "up")
        else:
            NODE_HEALTH.update_health(addr[0], "down")
        conn.close()

def heartbeat_timeout(node_urls):
    global NODE_HEALTH
    while True:
        node_health_copy = NODE_HEALTH.get_data()
        for url in node_urls:
            if node_health_copy[url] == "up":
                if (time.time() - NODE_HEALTH.last_heartbeat_time[url]) > 30:
                    NODE_HEALTH.update_health(url, "down")
        sleep(20)

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
        
    def get_all_ip_port(self):
        with self.lock:
            ret = []
            for node in self.node:
                name = next(iter(node))
                ret.append((node[name]["ip"], node[name]["port"]))
            return ret

    def get_port_for_ip(self, ip):
        with self.lock:
            for node in self.node:
                name = next(iter(node))
                if node[name]["ip"] == ip:
                    return node[name]["port"]
            return None

    def dump_data(self):
        with self.lock:
            return self.node

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
            global NODE_HEALTH
            node_count = len(self.task_to_ip[task])
            node_health_copy = NODE_HEALTH.get_data()
            while node_count:
                ip = self.task_to_ip[task][self.last_task_ip[task]]
                self.last_task_ip[task] = (self.last_task_ip[task] + 1) % len(self.task_to_ip[task])
                if node_health_copy[ip] == "up":
                    return ip
                node_count -= 1

            return "down"

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
    """
    Tentative data format:
        {
            "task": "task_name",
            "task_id": "task_id",
        }
    """


    data = json.loads(data)
    task = data["task"]
    task_id = data["task_id"]
    data["to_execute"] = False

    ip_dest = RR.get_ip(task)
    ip_list = NODE.get_all_ip_port()

    if ip_dest == "down":
        TASKQUEUE.add_task(task, task_id)
        return

    # Send task to node
    for ip in ip_list:
        if ip[0] == ip_dest:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip[0], ip[1]))
            data["to_execute"] = True
            sock.sendall(json.dumps(data).encode())
            sock.close()
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip[0], ip[1]))
            sock.sendall(json.dumps(data).encode())
            sock.close()

def server():
    # Listen for incoming headers and send to respective nodes or stores in queue
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', 8000))    # later change to master node IP as per yaml file
    server.listen(5)

    # handle incoming requests using different threads
    while True:
        conn, addr = server.accept()
        data = conn.recv(1024).decode()
        threading.Thread(target=handle_request, args=(data,)).start()

def handle_pending_tasks():
    while True:
        task = TASKQUEUE.get_task()
        if task:
            for t in task:
                TASKQUEUE.remove_task(t["task_id"])
                handle_request(json.dumps(t))
        sleep(15)

def main():
    global NODE, RR, TASKQUEUE, NODE_HEALTH
    NODE = Node()
    load_config()
    RR = RoundRobin(NODE)
    TASKQUEUE = TaskQueue()
    node_urls = []
    for ip in NODE.get_all_ip_port():
        node_urls.append(ip[0])

    NODE_HEALTH = Node_Health(node_urls)

    
    server_thread = threading.Thread(target=server)
    heartbeat_thread = threading.Thread(target=heartbeat_main, args=(node_urls,))
    pending_task_thread = threading.Thread(target=handle_pending_tasks)
    heartbeat_timer_thread = threading.Thread(target=heartbeat_timeout, args=(node_urls,))

    server_thread.start()
    heartbeat_thread.start()
    pending_task_thread.start()
    heartbeat_timer_thread.start()

    server_thread.join()
    heartbeat_thread.join()
    pending_task_thread.join()
    heartbeat_timer_thread.join()


# Global variables
# Node health imported from heart_beat.py
NODE = None
RR = None
TASKQUEUE = None
NODE_HEALTH = None

if __name__ == "__main__":
    main()