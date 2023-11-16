from flask import Flask, request, jsonify
from flask_cors import CORS

from services.classify_comments.inference import classify_text


app = Flask(__name__)
CORS(app)


@app.route("/api/classify", method=["POST"])
def classify() -> tuple[dict, int]:
    try:
        data = request.get_json()
        if "text" not in data:
            return jsonify({"error": "The 'text' field is missing in the payload"}), 400
        text = data["text"]
        result = classify_text(text)
        return jsonify(result), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True)
