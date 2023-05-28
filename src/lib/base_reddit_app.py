"""Instantiates an app on port 8080 for testing purposes.

For example, to access the Reddit API with proper credentials, an app on
http://localhost:8080 needs to exist (since this is our redirect URI)
"""
from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello():
    return 'Hello, World!'

if __name__ == "__main__":
    app.run(host='localhost', port=8080)
