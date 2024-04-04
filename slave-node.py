import threading
import socket
from asyncio import run
import yaml
from pathlib import Path
import json
import subprocess
import sqlite3
from enum import Enum
from time import sleep 
from utils import *
import asyncio,time

# required
NODE_NAME = "node1"

# global variables
NODE_IP=""
MASTER_IP=""
HEARTBEAT_PORT=0
slave = None
TASKS = None

# node db stores
'''
    {
        task_id: str,
        data: {
            type: str,
            response: str,
            action: str
        },
        host_ip: str,
        host_port: int
    }
'''


# master to slave
'''
    {
        task_id: str,
        task: str
        to_execute: int
    }
'''


# client to slave
'''
    {
        task_id: str,
        data: {
            type: str,
            response: str,
            action: str
        }
    }
'''

# Create a thread-local data container
mydata = threading.local()

def get_db():
    # Ensure a unique connection per thread
    if not hasattr(mydata, "conn"):
        mydata.conn = sqlite3.connect(f'task_{NODE_NAME}.db')
    return mydata.conn

class TaskList:
    def __init__(self) -> None:
        self.task_list = {}

    def add_task(self, task_name: str, type: str, response: str, action: str, parameters: dict):
        self.task_list[task_name] = {"type":type, "response":response, "action": action, "parameters": parameters}
        
    def get_task_names(self):
        return [i for i in self.task_list.keys()]
    
    def get_task_parameter_names(self, task_name: str):
        return [i for i in self.task_list[task_name]["parameters"].keys()]
    
    def get_task_parameters(self, task_name: str):
        return self.task_list[task_name]["parameters"]
    
    def get_task_action(self, task_name: str):
        return self.task_list[task_name]["action"]
    
    
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
        conn = get_db()
        c = conn.cursor()
        c.execute(f'''CREATE TABLE IF NOT EXISTS {NodeTaskQueue.dbname}
             (task_id text, data text, host_ip text, host_port int)''')
        c.execute(f'''CREATE UNIQUE INDEX IF NOT EXISTS task_id_index
             ON {NodeTaskQueue.dbname} (task_id)''')
        conn.commit()

    def add_task(self, task_id: str, data: str, host_ip: str, host_port: int):
        with self.lock:
            try:
                conn = get_db()
                c = conn.cursor()
                print(task_id)
                print(data)
                print(host_ip)
                print(host_port)
                c.execute(f"INSERT INTO {NodeTaskQueue.dbname} VALUES (?, ?, ?, ?)", (task_id, str(data), host_ip, host_port))
                conn.commit()
            except sqlite3.IntegrityError:
                return "Task already exists in the queue."

    def get_all_task(self):
        with self.lock:
            try:
                conn = get_db()
                c = conn.cursor()
                c.execute(f"SELECT data FROM {NodeTaskQueue.dbname}")
                return c.fetchall()
            except sqlite3.IntegrityError:
                return "Task does not exist in the queue."
    
    def get_task(self, task_id: str):
        with self.lock:
            try:
                # TODO: modify below to include host_ip, host_port
                # self.c.execute(f"SELECT data, host_ip, host_port FROM {NodeTaskQueue.dbname} WHERE task_id=?", (task_id,))
                conn = get_db()
                c = conn.cursor()
                c.execute(f"SELECT data FROM {NodeTaskQueue.dbname} WHERE task_id=?", (task_id,))
                res = c.fetchone() 
                return res[0]
            except sqlite3.IntegrityError:
                return "Task does not exist in the queue."
    
    def remove_task(self, task_id: str):
        with self.lock:
            try:
                conn = get_db()
                c = conn.cursor()
                c.execute(f"DELETE FROM {NodeTaskQueue.dbname} WHERE task_id=?", (task_id,))
                conn.commit()
            except sqlite3.IntegrityError:
                return "Task does not exist in the queue."

    def __del__(self):
        conn = get_db()
        conn.close()

class Slave:
    def __init__(self, port: int) -> None:
        self.name = NODE_NAME
        self.port = port
        self.task_list = TaskList()
        self.request_queue = NodeTaskQueue(f'task_{NODE_NAME}')
        self.lock = threading.Lock()
        self.processed_tasks = {}
            
    def get_port(self):
        return self.port
    
    def get_node_name(self):
        return self.name
    
    def get_node_tasks(self):
        return self.task_list.get_task_names()
    
    def add_node_task(self, task_name: str, type: str, response: str, action: str, parameters: dict):
        with self.lock:
            self.task_list.add_task(task_name, type, response, action, parameters)
    
    def pop_task_id(self, task_id : str):
        with self.lock:
            self.request_queue.remove_task(task_id)
    
    async def handle_request(self, data: str, host_ip: str, host_port: int):
        # parses data from client and stores in the queue
        with self.lock:
            res = json.loads(data)
            self.request_queue.add_task(res["task_id"],json.dumps(res["data"]),host_ip,host_port)
    
    def add_processed_task(self, task_id: str, res: str):
        self.processed_tasks[task_id] = res
    
    async def get_processed_task(self, task_id: str):
        start_time = time.time()
        with self.lock:
            while True:
                if(time.time() - start_time > 5):
                    return None
                if(task_id in self.processed_tasks.keys()):
                    return self.processed_tasks[task_id]
                else:
                    sleep(.01)
            
        
    def process_bash_task(self, action: str ):
        output = str(subprocess.check_output(action, shell=True))
        return output
    
    async def process_task(self, task_id: str):
        with self.lock:
            task = json.loads(self.request_queue.get_task(task_id))
            print("task : ", task)
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
                res =  self.process_bash_task(task["action"])
                self.add_processed_task(task_id, res)
                
            return None

    async def handle_data(self, data: str, host_ip: str, host_port: int):
        # either data from client or master
        with self.lock:
            res = json.loads(data)
            for key in res.keys():
                if key == "data":
                    self.handle_request(data, host_ip, host_port)
                    out = await self.get_processed_task(res["task_id"])
                    if out != None:
                        return { "from": "client", "status": "200", "response": out}
                    else:
                        return { "from": "client", "status": "404", "response": out} 
                elif key == "task":
                    # master
                    await self.handle_task(int(res["task_id"]),res["task"],res["to_execute"])
                    return {"from": "master"}
            return None
        
    async def handle_task(self, task_id: str, task: str, to_execute: str):
        # handles the task given from the master
        # or pops it from queue
        with self.lock:
            if (int(to_execute) == 1):
                if (task in self.get_node_tasks()):
                    # TODO: Change this
                    print(self.process_task(task_id))
                else:
                    print(f"Task: {task} not supported.")
            
            # pop the task_id
            self.pop_task_id(task_id)
        
        
def load_config():
    # load yaml
    global slave,TASKS,MASTER_IP,HEARTBEAT_PORT,NODE_IP
    config_path = Path("config.yaml")
    if not config_path.exists():
        print("config.yaml not found.") 
        return 0
    with open('config.yaml') as file:
        config = yaml.full_load(file)
        for node in config["nodes"]:
            name = next(iter(node))
            if(name == NODE_NAME):
                NODE_IP = node[name]["ip"]
                break
        else:
            print(f"NODE_NAME: {NODE_NAME} not found in config.")
            return 0
            
        MASTER_IP = config["master"]["ip"]
        HEARTBEAT_PORT = config["heartbeat-port"]
        
        for node in config["nodes"]:
            node_name = next(iter(node))
            if(node_name == NODE_NAME):
                port = int(node[node_name]["port"])
                tasks = node[node_name]["tasks"]
                slave = Slave(port=port)
                
                for task in config["tasks"]:
                    task_name = next(iter(task))
                    if task_name not in tasks:
                        continue 
                    task_type = task[task_name]["type"]
                    task_response = task[task_name]["response"]
                    task_action = task[task_name]["action"]
                    task_parameters = dict()
                    if "parameters" in task[task_name].keys():
                        for param in task[task_name]["parameters"]:
                            param_name = next(iter(param))
                            param_type = param[param_name]["type"]
                            
                            task_parameters[param_name] = param_type
                    params_in_action=set(get_parameters_from_action(task_action))
                    
                    if params_in_action != set([i for i in task_parameters.keys()]):
                        print(f"Parameters mismatch in task: {task_name}")
                        return 0
                    
                    slave.add_node_task(task_name,task_type,task_response, task_action, task_parameters)
                                          
                break
        else:
            print("NODE_NAME: {} not found in the config.yaml".format(NODE_NAME))
            return 0
    
    return 1

    
async def slave_server():
    global slave
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((NODE_IP, slave.get_port())) 
    server.listen(5)
    print("started listening on: ", slave.get_port())

    while True:
        conn, addr = server.accept()
        data = conn.recv(1024)
        host, port = conn.getpeername()
        print(f"recieved from ip: {host} port: {port}")
        print(data)
        res = await slave.handle_data(data,host,port)
        if(res["from"] == "master"):
            conn.close()
        else:
            conn.sendall(res)
            conn.close()

def start_health():
    while True:
        try:
            heartbeat = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            heartbeat.connect((MASTER_IP, HEARTBEAT_PORT))
            heartbeat.sendall(b'{"status":"up"}')
            heartbeat.close()
        except:
            pass
        sleep(15)
        

def main():
    if not load_config():
        return
      
    slave_server_thread = threading.Thread(target=slave_server)
    start_health_thread = threading.Thread(target=start_health)

    slave_server_thread.start()
    start_health_thread.start()
    
    slave_server_thread.join()
    start_health_thread.join()

if __name__ == "__main__":
    main()