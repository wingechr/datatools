import logging

from flask import Flask  # , request

# application only for production webserver
app = application = Flask(__name__)


@app.route("/", methods="POST")
def index():
    """return html"""
    response = "<h1>Hello Markus !!</h1>"
    return response


if __name__ == "__main__":
    logging.basicConfig(
        format="[%(asctime)s %(levelname)7s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG,
    )
    app.run(host="localhost", port=5000, debug=True)
