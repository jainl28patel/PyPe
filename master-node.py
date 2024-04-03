import threading
import socket
from time import sleep
from asyncio import run
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
        for url in node_urls:
            self.node_urls[url] = "down"
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

# Function to check the health of a node
async def check_node_health(node_url, session):
    try:
        async with session.get(node_url) as response:
            if response.status == 200:
                return "up"
            else:
                return "down"
    except aiohttp.ClientError:
        return "down"

# Main function to orchestrate the heartbeat checks
async def heartbeat_main(node_urls):
    async with aiohttp.ClientSession() as session:
        while True:
            global NODE_HEALTH
            # Asynchronously check the health of each node
            tasks = [check_node_health(url, session) for url in node_urls]
            health_results = await asyncio.gather(*tasks)

            # Update the NODE_HEALTH dictionary with the new health results
            new_nodes_health = dict(zip(node_urls, health_results))

            # Check if there's a change in the health status
            previous_nodes_health = NODE_HEALTH.get_data()
            if new_nodes_health != previous_nodes_health:
                # update the NODE_HEALTH dictionary
                for url, status in new_nodes_health.items():
                    if status != previous_nodes_health[url]:
                        NODE_HEALTH.update_health(url, status)

            # Wait for some time before the next check
            await asyncio.sleep(10)  # Check every 10 seconds


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
        
    def get_all_ip(self):
        with self.lock:
            ret = []
            for node in self.node:
                name = next(iter(node))
                ret.append(node[name]["ip"])
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
    ip_list = NODE.get_all_ip()

    # TODO: Add a service that periodically checks if ANY node that execute task is up
    if ip_dest == "down":
        TASKQUEUE.add_task(task, task_id)
        return

    # Send task to node
    for ip in ip_list:
        if ip == ip_dest:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip, 8000)) # later change to node IP as per yaml file
            data["to_execute"] = True
            sock.sendall(json.dumps(data).encode())
            sock.close()
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((ip, 8000)) # later change to node IP as per yaml file
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

def heartbeat(node_urls):
    # responsible_node_url = "http://127.0.0.1:5000/report"
    # node_urls = [
    #     "https://github.com/iiteen",
    #     "https://github.com/wadetb/heartbeat",
    #     "http://127.0.0.1:80/",
    #     # Add more node URLs as needed
    # ]
    run(heartbeat_main(node_urls))

def handle_pending_tasks():
    while True:
        print("Checking for pending tasks...")
        task = TASKQUEUE.get_task()
        if task:
            for t in task:
                handle_request(json.dumps(t))

        sleep(5)

def main():
    global NODE, RR, TASKQUEUE, NODE_HEALTH
    NODE = Node()
    load_config()
    RR = RoundRobin(NODE)
    TASKQUEUE = TaskQueue()
    node_urls = []
    for ip in NODE.get_all_ip():
        node_urls.append(ip)

    print(node_urls)
    NODE_HEALTH = Node_Health(node_urls)

    
    server_thread = threading.Thread(target=server)
    heartbeat_thread = threading.Thread(target=heartbeat, args=(node_urls,))
    pending_task_thread = threading.Thread(target=handle_pending_tasks)

    server_thread.start()
    heartbeat_thread.start()
    pending_task_thread.start()

    server_thread.join()
    heartbeat_thread.join()
    pending_task_thread.join()


# Global variables
# Node health imported from heart_beat.py
NODE = None
RR = None
TASKQUEUE = None
NODE_HEALTH = None

if __name__ == "__main__":
    main()