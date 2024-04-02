from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route("/report", methods=["POST"])
def report_health():
    health_data = request.get_json()
    print("Received health data:")
    print(health_data)
    return jsonify({"status": "success"}), 200


if __name__ == "__main__":
    app.run(debug=True,port=5000)
