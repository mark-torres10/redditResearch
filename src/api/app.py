import os

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS

# from services.classify_comments.inference import classify_text


load_dotenv()

flask_app = os.getenv("FLASK_APP")
flask_run_port = os.getenv("FLASK_RUN_PORT")
flask_run_host = os.getenv("FLASK_RUN_HOST")

app = Flask(__name__)
CORS(app)
# app.config["FLASK_APP"] = flask_app


def classify_text() -> int:
    return {"prob": 0.9, "label": 1}


@app.route("/api/classify", methods=["POST"])
def classify() -> tuple[dict, int]:
    try:
        data = request.get_json()
        if "text" not in data:
            return jsonify({"error": "The 'text' field is missing in the payload"}), 400
        text = data["text"]
        result = classify_text(text)
        res = {**result, "text": text}
        return jsonify(res), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # app.run(port=int(flask_run_port), host=flask_run_host, debug=True)
    app.run(debug=True)
