import json

import langchain
import openai
import pandas as pd

from lib.db.sql.helper import load_query_as_df


# TODO: add schemas here.
base_prompt = """
TODO: add schemas here.

Generate the SQL query to answer the following question. Only questions that \
can be answered with a SELECT query are valid. Any questions requiring other
database operations (e.g., insert, overwrite, delete, etc.) are strictly
forbidden as the database is read-only. Return your response as a JSON in the \
following format, with keys "valid_question", "sql_query", and "reason":
{
    "valid_question": boolean, whether or not the question can be answered \
    using the SQL tables and schemas provided.
    "sql_query": string, the SQL query to answer the question. Only return \
    queries that can be answered with a "SELECT". The table is read-only. \
    Any updates or inserts or deletes are strictly forbidden. Any queries \
    that would require these operations are invalid (and valid_question \
    should be marked as False accordingly).
    "reason": if valid_question is False, a string explaining why (return "" \
    if valid_question is True). Can have only two possible values: (1) \
    "invalid question" if the question is not one relevant to the database, \
    cannot be answered in SQL, or is not relevant to the topic, or (2) \
    "no answer" if the question is valid but cannot be answered using the \
    provided tables and schemas.
}
```
{question}
```

"""

interpret_db_response_prompt = """
TODO: add schemas here.

You ran the following query in the database:
```
{query}
```

You got the following response:
```
{db_response}
```

Given this response, return a human-friendly response, in the tone of a \
data analyst, that explains the response. Return your response as a JSON in \
the following format:
{
    "response": string, the human-friendly response. Return at most \
    3 sentences.
}
"""
def send_query_to_gpt(prompt: str) -> str:
    pass


def get_possible_sql_query(question: str) -> str:
    """Take a possible question and pass to ChatGPT to get the corresponding
    SQL query, if applicable.
    
    Returns either the SQL query or an empty string if the question is invalid.
    
    Validates the query (e.g., checks that it is proper SQL and that it has
    a SELECT clause). 
    """
    prompt = base_prompt.format(question=question)
    response = send_query_to_gpt(prompt)
    response = json.loads(response)
    # TODO: check that query is a SELECT query
    return response["sql_query"] if response["valid_question"] else ""
    

def interpret_sql_query(query: str, df: pd.DataFrame) -> str:
    prompt = interpret_db_response_prompt.format(
        query=query,
        db_response=df.to_string()
    )
    response = send_query_to_gpt(prompt)
    response = json.loads(response)
    return response["response"]


# TODO: rewrite in Langchain
# TODO: run first prompt to get relevant SQL query. Run SQL query in DB. Get
# result. Then pass it back to ChatGPT to interpret
def answer_question(question: str) -> str:
    """Answer a given question."""
    sql_query = get_possible_sql_query(question)
    df = load_query_as_df(sql_query)
    response = interpret_sql_query(query=sql_query, df=df)
    return response


def query_chatgpt() -> None:
    pass
