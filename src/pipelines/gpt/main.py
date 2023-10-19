"""Query our DB using ChatGPT API to generate SQL queries"""
from lib.helper import track_function_runtime
from services.query_gpt.handler import main as query_gpt

@track_function_runtime
def main(event: dict) -> None:
    query_gpt(event)


if __name__ == "__main__":
    event = {"query": "How many comments have we synced?"}
    main(event)
