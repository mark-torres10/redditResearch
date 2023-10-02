"""Helper utilities for interacting with Postgres."""
from dotenv import load_dotenv
import os

import numpy as np
import pandas as pd
import psycopg2

current_file_directory = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.abspath(os.path.join(current_file_directory, "../../../../.env"))
load_dotenv(dotenv_path=env_path)

DB_PARAMS = {
    'host': "localhost",
    'port': "5432",
    'database': "reddit_data",
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD")
}

conn = psycopg2.connect(**DB_PARAMS)
cursor = conn.cursor()


def generate_create_table_statement(table_name: str) -> str:
    pass


def convert_python_dtype_to_sql_type(dtype) -> str:
    """Converts a Python dtype to a SQL type."""
    # https://www.psycopg.org/docs/usage.html#adaptation-of-python-values-to-sql-types
    instance_type = ""
    if isinstance(dtype, np.object_):
        instance_type = "text"
    elif isinstance(dtype, np.bool_):
        instance_type = "bool"
    elif isinstance(dtype, np.int64):
        instance_type = "int"
    elif isinstance(dtype, np.float64):
        instance_type = "float"
    else:
        raise ValueError(f"Unknown dtype: {dtype}")

    return instance_type

def generate_create_table_statement_from_df(
    df: pd.DataFrame, table_name: str
) -> str:
    """Given a df, generate a create table statement.
    
    Infers the columns and dtypes from the pandas df.
    """
    schema = [(col, df[col].dtype) for col in df.columns]
    create_table_sql = f"CREATE TABLE {table_name} ("
    for col, dtype in schema:
        sql_type = convert_python_dtype_to_sql_type(dtype)
        create_table_sql += f"{col} {sql_type}, "
    create_table_sql = create_table_sql.rstrip(', ') + ");"
    return create_table_sql


def create_table() -> None:
    pass


def check_if_table_exists() -> None:
    pass


def write_to_database() -> None:
    pass


def get_column() -> None:
    pass


def single_row_insertion_is_valid() -> None:
    pass


def get_all_table_results_as_df() -> None:
    pass


def create_new_table_from_df(
    df: pd.DataFrame, table_name: str
) -> None:
    """Given a df, create a new table from it.
    
    Infers the columns and dtypes from the pandas df.
    """
    try:
        # create table
        create_table_statement = generate_create_table_statement_from_df(
            df=df, table_name=table_name
        )
        cursor.execute(create_table_statement)
        conn.commit()
    except Exception as e:
        print(f"Unable to create table {table_name}: {e}")
        raise


def write_df_to_database(
    df: pd.DataFrame, table_name: str
) -> None:
    """Writes a dataframe to a Postgres table.
    
    Assumes that the column names of the dataframe are the same as the column
    match that of the table schema.
    """
    try:
        # check to see if table exists. If not, create it
        cursor.execute(
            f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='{table_name}');"
        )
        table_exists = cursor.fetchone()[0]
        if not table_exists:
            create_new_table_from_df(df=df, table_name=table_name)
        df.to_sql(table_name, conn, if_exists="append", index=False)
    except Exception as e:
        print(f"Unable to write df to {table_name}: {e}")
        raise


if __name__ == "__main__":
    cursor.execute(
        "CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);"
    )
    cursor.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))
