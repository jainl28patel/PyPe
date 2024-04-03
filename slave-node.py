import threading
import socket
from asyncio import run
import yaml
from pathlib import Path
import json
import subprocess

# required
NODE_NAME = "node1"

# global variables
slave = None

class Slave:
    def __init__(self) -> None:
        self.name = NODE_NAME
        self.port = 0
        self.task = ""
        self.request_queue = {}

    def __init__(self, port: int, task: str) -> None:
        # TODO: add multiple tasks
        self.name = NODE_NAME
        self.port = port
        self.task = task
        self.request_queue = {}

    def get_port(self):
        return self.port
    
    def get_node_name(self):
        return self.name
    
    def get_task(self):
        return self.task
    
    def pop_req_id(self, req_id : int):
        self.request_queue.pop(req_id)
    
    def handle_request(self, data: str):
        # parses data from client and stores in the queue
        # sample_data = {"req_id": 22, "data": "echo 'hi'" }
        res = json.loads(data)
        self.request_queue[res["req_id"]] = res["data"]
    
    def process_task(self, req_id: int, task: str):
        # TODO: Change this to handle actual task
        output = subprocess.check_output(self.request_queue[req_id], shell=True)
        return output

    def handle_data(self, data: str):
        # either data from client or master
        res = json.loads(data)
        for key in res.keys():
            if key == "data":
                self.handle_request(data)
                break
            elif key == "task":
                # master
                self.handle_task(int(res["req_id"]),res["node_name"],res["task"])
                break
        
    def handle_task(self, req_id: int, node_name: str, task: str):
        # handles the task given from the master
        # or pops it from queue
        if (node_name == self.name):
            if (task == self.task):
                # TODO: Change this
                print(self.process_task(req_id,task))
            else:
                print(f"Task: {task} not supported.")
        
        # pop the req_id
        self.pop_req_id(req_id)
        
        
def load_config():
    # load yaml
    global slave
    config_path = Path("config.yaml")
    if not config_path.exists():
        print("config.yaml not found.") 
        return
    with open('config.yaml') as file:
        config = yaml.full_load(file)
        for node in config["nodes"]:
            node_name = next(iter(node))
            if(node_name == NODE_NAME):
                port = int(node[node_name]["port"])
                task = node[node_name]["task"]
                slave = Slave(port=port,task=task) 
                break
        else:
            print("NODE_NAME: {} not found in the config.yaml".format(NODE_NAME))
            return 0
    
    return 1

    
def slave_server():
    global slave
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', slave.get_port())) 
    server.listen(5)

    while True:
        conn, addr = server.accept()
        data = conn.recv(1024)
        slave.handle_data(data)
        conn.close()
    
def main():
    load_config()
    
    slave_server_thread = threading.Thread(target=slave_server)

    slave_server_thread.start()
    
    slave_server_thread.join()

if __name__ == "__main__":
    main()