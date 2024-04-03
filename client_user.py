from flask import Flask, request, jsonify
from client import post_to_master, post_to_nodes, get_from_nodes

app = Flask(__name__)

node_urls = {}
master_url = ""

@app.route("/", methods=["GET"])
async def user_get_request():
    node_response = await get_from_nodes(node_urls)
    return jsonify(node_response), 200


@app.route("/", methods=["POST"])
async def user_post_request():
    await post_to_master(request.get_json(), master_url)
    await post_to_nodes(request.get_json(), node_urls)
    return 200

if( __name__ == "__main__"):
    app.run(debug=True)
