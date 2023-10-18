"""Query DB using ChatGPT.

https://python.langchain.com/docs/use_cases/qa_structured/sql
https://python.langchain.com/docs/expression_language/cookbook/sql_db
https://python.langchain.com/docs/use_cases/question_answering/
https://python.langchain.com/docs/get_started/quickstart
https://dev.to/ngonidzashe/speak-your-queries-how-langchain-lets-you-chat-with-your-database-p62
"""
from dotenv import load_dotenv
import json
import os

from langchain.agents import create_sql_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.agents.agent_types import AgentType
from langchain.chat_models import ChatOpenAI
from langchain.sql_database import SQLDatabase
import pandas as pd

from lib.db.sql.helper import db_uri, load_query_as_df

current_file_directory = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.abspath(os.path.join(current_file_directory, "../../../.env")) # noqa
load_dotenv(dotenv_path=env_path)
openai_api_key = os.getenv("OPENAI_API_KEY")

# https://python.langchain.com/docs/integrations/toolkits/sql_database#initialization
# https://python.langchain.com/docs/use_cases/qa_structured/sql#case-3-sql-agents
llm = ChatOpenAI(
    openai_api_key=openai_api_key, model_name="gpt-3.5-turbo", temperature=0
)
db = SQLDatabase.from_uri(db_uri)
toolkit = SQLDatabaseToolkit(db=db, llm=llm) # note: example uses OpenAI not ChatOpenAI, unsure if this matters?
agent_executor = create_sql_agent(
    llm=llm,
    toolkit=toolkit,
    verbose=True,
    agent_type=AgentType.ZERO_SHOT_REACT_DESCRIPTION,
)

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


# TODO: rewrite in Langchain. Use SQL object
# https://python.langchain.com/docs/use_cases/qa_structured/sql#case-3-sql-agents
# TODO: run first prompt to get relevant SQL query. Run SQL query in DB. Get
# result. Then pass it back to ChatGPT to interpret
def answer_question(question: str) -> str:
    """Answer a given question."""
    sql_query = get_possible_sql_query(question)
    df = load_query_as_df(sql_query)
    response = interpret_sql_query(query=sql_query, df=df)
    return response


def query_chatgpt(query: str) -> None:
    agent_executor.run(query)


if __name__ == "__main__":
    query = input("Please enter a question:\t")
    query_chatgpt(query)
