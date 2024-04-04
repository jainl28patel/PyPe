import socket
import json
import asyncio
from flask import Flask, request, jsonify
import uuid
from pathlib import Path
import yaml

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


async def send_data_to_socket(url, data):
    host, port = url.split(":")
    reader, writer = await asyncio.open_connection(host, int(port))
    writer.write(data.encode())
    await writer.drain()
    response = await reader.read(100)
    writer.close()
    await writer.wait_closed()
    return response


@app.route("/", methods=["GET"])
def user_get_request():
    tasks = [send_data_to_socket(node_url, "GET") for node_url in NODES_URL]
    node_responses = asyncio.run(asyncio.gather(*tasks))
    return jsonify([response.decode() for response in node_responses]), 200


@app.route("/", methods=["POST"])
async def user_post_request():
    request_data = request.get_json()
    task, task_id, search_text = request_parser(request_data)
    await send_data_to_socket(
        MASTER_URL,
        json.dumps({"task_id": task_id, "task": task}),
    )
    tasks = [
        asyncio.create_task(
            send_data_to_socket(
                node_url,
                json.dumps({"task_id": task_id, "data": {"search_text": search_text}}),
            )
        )
        for node_url in NODES_URL
    ]
    for task in asyncio.as_completed(tasks):
        response = await task
        response_data = json.loads(response.decode())
        if response_data.get("status") == "200":
            for t in tasks:
                t.cancel()  # cancel all other tasks
            return jsonify(response_data), 200
    return jsonify({"message": "Task cannot be performed"}), 500


def load_config():
    # load yaml
    global NODES_IP,MASTER_IP
    config_path = Path("config.yaml")
    if not config_path.exists():
        print("config.yaml not found.") 
        return 0
    with open('config.yaml') as file:
        config = yaml.full_load(file)
        
        MASTER_URL = f"{config["master"]["ip"]}:{config["master"]["port"]}" 
        
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
