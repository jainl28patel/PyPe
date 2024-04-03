from flask import Flask

app = Flask(__name__)


@app.route("/", methods=["GET"])
def home():
    return "Hello, this is the response to your GET request!",200


if __name__ == "__main__":
    # app.run(debug=True, port=5000)
    app.run(port=9000)
