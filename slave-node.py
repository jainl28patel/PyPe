import threading
import socket
from asyncio import run
import yaml
from pathlib import Path
import json
import subprocess
import sqlite3
from enum import Enum

# required
NODE_NAME = "node1"

# global variables
slave = None

'''
    {
        task_id: str,
        data: {
            type: str,
            response: str,
            action: str
        },
        client_ip: str
    }
'''
class TaskType(Enum):
    BASH = 1
    PYTHON = 2
    PYTHON3 = 3
    UNDEFINED = 4

class NodeTaskQueue:
    dbname = ""
    def __init__(self, db_name: str) -> None:
        NodeTaskQueue.dbname = f'{db_name}'
        
        self.lock = threading.Lock()
        self.conn = sqlite3.connect(f"{NodeTaskQueue.dbname}.db")
        self.c = self.conn.cursor()
        self.c.execute(f'''CREATE TABLE IF NOT EXISTS {NodeTaskQueue.dbname}
             (task_id text, data text, client_ip str)''')
        self.c.execute(f'''CREATE UNIQUE INDEX IF NOT EXISTS task_id_index
             ON {NodeTaskQueue.dbname} (task_id)''')
        self.conn.commit()

    def add_task(self, task_id: str, data: str, client_ip: str):
        with self.lock:
            try:
                self.c.execute(f"INSERT INTO {NodeTaskQueue.dbname} VALUES (?, ?, ?)", (task_id, data, client_ip))
                self.conn.commit()
            except sqlite3.IntegrityError:
                return "Task already exists in the queue."

    def get_all_task(self):
        with self.lock:
            try:
                self.c.execute(f"SELECT data FROM {NodeTaskQueue.dbname}")
                return self.c.fetchall()
            except sqlite3.IntegrityError:
                return "Task does not exist in the queue."
    
    def get_task(self, task_id: str):
        with self.lock:
            try:
                self.c.execute(f"SELECT data, client_ip FROM {NodeTaskQueue.dbname} WHERE task_id=?", (task_id,))
                res = self.c.fetchone() 
                return res[0]
            except sqlite3.IntegrityError:
                return "Task does not exist in the queue."
    
    def remove_task(self, task_id: str):
        with self.lock:
            try:
                self.c.execute(f"DELETE FROM {NodeTaskQueue.dbname} WHERE task_id=?", (task_id,))
                self.conn.commit()
            except sqlite3.IntegrityError:
                return "Task does not exist in the queue."

    def __del__(self):
        self.conn.close()

class Slave:
    def __init__(self) -> None:
        self.name = NODE_NAME
        self.port = 0
        self.task = ""
        self.request_queue = NodeTaskQueue(f'task_{NODE_NAME}')

    def __init__(self, port: int, task: str) -> None:
        # TODO: add multiple tasks
        self.name = NODE_NAME
        self.port = port
        self.task = task
        self.request_queue = NodeTaskQueue(f'task_{NODE_NAME}')

    def get_port(self):
        return self.port
    
    def get_node_name(self):
        return self.name
    
    def get_task(self):
        return self.task
    
    def pop_task_id(self, task_id : str):
        self.request_queue.remove_task(task_id)
    
    def handle_request(self, data: str):
        # parses data from client and stores in the queue
        res = json.loads(data)
        self.request_queue.add_task(res["task_id"],res["data"])
    
    def process_bash_task(self, action: str ):
        output = subprocess.check_output(action, shell=True)
        return output
    
    def process_task(self, task_id: str):
        task = json.loads(self.request_queue.get_task(task_id))
        task_type = None

        if task["type"]== "bash":
            task_type = TaskType.BASH
        elif task["type"]== "python":
            task_type = TaskType.PYTHON
        elif task["type"]== "python3":
            task_type = TaskType.PYTHON3
        else:
            task_type = TaskType.UNDEFINED
        
        if(task_type == TaskType.BASH):
            return self.process_bash_task(task["action"])
        
        return None

    def handle_data(self, data: str):
        # either data from client or master
        res = json.loads(data)
        for key in res.keys():
            if key == "data":
                self.handle_request(data)
                break
            elif key == "task":
                # master
                self.handle_task(int(res["task_id"]),res["node_name"],res["task"])
                break
        
    def handle_task(self, task_id: str, node_name: str, task: str):
        # handles the task given from the master
        # or pops it from queue
        if (node_name == self.name):
            if (task == self.task):
                # TODO: Change this
                print(self.process_task(task_id))
            else:
                print(f"Task: {task} not supported.")
        
        # pop the task_id
        self.pop_task_id(task_id)
        
        
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
    print("started listening on: ", slave.get_port())

    while True:
        conn, addr = server.accept()
        print("sssss")
        data = conn.recv(1024)
        host, port = conn.getpeername()
        print("ip: ",host,port)
        print(data)
        slave.handle_data(data)
        
        conn.close()
    
def main():
    load_config()
    slave_server()
    
    # slave_server_thread = threading.Thread(target=slave_server)

    # slave_server_thread.start()
    
    # slave_server_thread.join()

if __name__ == "__main__":
    main()