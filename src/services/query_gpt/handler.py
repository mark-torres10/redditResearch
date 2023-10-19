"""Query ChatGPT using OpenAI API."""
from services.query_gpt.helper import query_chatgpt

def main(event: dict, context: dict) -> None:
    query = event["query"]
    query_chatgpt(query)
