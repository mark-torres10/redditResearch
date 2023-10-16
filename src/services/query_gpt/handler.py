"""Query ChatGPT using OpenAI API."""
from services.query_gpt.helper import query_chatgpt

def handler(event: dict, context: dict) -> None:
    query_chatgpt()
