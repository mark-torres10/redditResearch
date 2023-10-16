"""Query our DB using ChatGPT API to generate SQL queries"""
from lib.helper import track_function_runtime
from services.query_gpt.handler import main as query_gpt

@track_function_runtime
def main() -> None:
    query_gpt()


if __name__ == "__main__":
    pass
