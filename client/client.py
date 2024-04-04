import socket
import json
import asyncio
from flask import Flask, request, jsonify
import uuid
from pathlib import Path
import yaml
import threading

app = Flask(__name__)


NODES_URL=[]
MASTER_URL = ""

def request_parser(request):
    if isinstance(request, dict):
        search_text = request["search-text"]
    else:
        try:
            search_text = (json.loads(request))["search-text"]
        except TypeError:
            print("The request must be a dictionary or a JSON-formatted string.")
            return None, None
    # do this later
    task_id = str(uuid.uuid4())
    task = "t1"
    return (task, task_id, search_text)


def send_data_to_socket(url, data):

    host, port = url.split(":")
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect((host, int(port)))
    conn.sendall(data.encode())
    print("url", url)
    return conn


@app.route("/", methods=["GET"])
def user_get_request():
    tasks = [send_data_to_socket(node_url, "GET") for node_url in NODES_URL]
    node_responses = asyncio.run(asyncio.gather(*tasks))
    return jsonify([response.decode() for response in node_responses]), 200


async def receive_conn(conn):
    resp = conn.recv(1024)
    resp_data = json.loads(resp.decode())
    return resp_data


@app.route("/", methods=["POST"])
async def user_post_request():
    request_data = request.get_json()
    task, task_id, search_text = request_parser(request_data)
    # print(NODES_URL)
    tasks_conn_object = [
        send_data_to_socket(
            node_url,
            json.dumps({"task_id": task_id, "data": {"search_text": search_text}}),
        )
        for node_url in NODES_URL
    ]

    # for node in NODES_URL:
    #     threading.Thread(target=)

    send_data_to_socket(
        MASTER_URL,
        json.dumps({"task_id": task_id, "task": task}),
    )

    for conn in tasks_conn_object:
        resp_data = await receive_conn(conn)
        print(resp_data)
        if resp_data.get("status") == "200":
            for conn2 in tasks_conn_object:
                conn2.close()
            return jsonify(resp_data), 200

    # for task in asyncio.as_completed(tasks):
    #     response = await task
    #     response_data = json.loads(response.decode())
    #     if response_data.get("status") == "200":
    #         for t in tasks:
    #             t.cancel()  # cancel all other tasks
    #         return jsonify(response_data), 200

    return jsonify({"message": "Task cannot be performed"}), 500


def load_config():
    # load yaml
    global NODES_URL, MASTER_URL
    config_path = Path("config.yaml")
    if not config_path.exists():
        print("config.yaml not found.") 
        return 0
    with open('config.yaml') as file:
        config = yaml.full_load(file)

        MASTER_URL = f'{config["master"]["ip"]}:{config["master"]["port"]}'

        for node in config["nodes"]:
            name = next(iter(node))
            node_ip = node[name]["ip"]    
            node_port = node[name]["port"]    
            NODES_URL.append(f"{node_ip}:{node_port}")

    return 1

if __name__ == "__main__":
    if not load_config():
        print("error in loading config.")
    else:
        app.run(port=8045)


# {
#     "task_id":uuid_string
#     "data":{
#         "search-text":hfskjhsjkf
#     }
# }
