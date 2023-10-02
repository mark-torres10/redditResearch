"""Helper utilities for interacting with Postgres."""
from dotenv import load_dotenv
import os
from typing import Optional

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


def convert_python_dtype_to_sql_type(dtype: np.dtype) -> str:
    """Converts a Python dtype to a SQL type."""
    # https://www.psycopg.org/docs/usage.html#adaptation-of-python-values-to-sql-types
    instance_type = ""
    if dtype == np.dtype('O'):
        instance_type = "text"
    elif dtype == np.dtype("bool"):
        instance_type = "bool"
    elif dtype == np.dtype("int64"):
        instance_type = "int"
    elif dtype == np.dtype("float64"):
        instance_type = "float"
    else:
        raise ValueError(f"Unknown dtype: {dtype}")

    return instance_type


def create_foreign_key_statements() -> Optional[list[str]]:
    return []

# https://www.postgresqltutorial.com/postgresql-python/create-tables/
def generate_create_table_statement(
    table_name: str,
    field_to_sql_type_map: dict,
    primary_keys: Optional[list[str]]
) -> str:
    primary_keys = []
    fields_list = [
        f"{field_name} {field_type} NOT NULL"
        for field_name, field_type in field_to_sql_type_map.items()
    ]
    fields_query = ',\n'.join(fields_list)
    primary_keys = f",PRIMARY KEY ({', '.join(primary_keys)})"
    foreign_keys = ',\n'.join(create_foreign_key_statements()) # TODO: need to implement
    create_table_sql = f"""CREATE TABLE {table_name} (
        {fields_query},
        {primary_keys},
        {foreign_keys}
    )
    """
    return create_table_sql
   

def generate_create_table_statement_from_df(
    df: pd.DataFrame, table_name: str, primary_keys: Optional[list] = None
) -> str:
    """Given a df, generate a create table statement.
    
    Infers the columns and dtypes from the pandas df.
    """
    col_to_dtype_map = {
        col: df[col].dtype
        for col in df.columns
    }
    col_to_sql_type_map = {
        col: convert_python_dtype_to_sql_type(dtype)
        for col, dtype in col_to_dtype_map.items()
    }

    create_table_sql = generate_create_table_statement(
        table_name=table_name,
        field_to_sql_type_map=col_to_sql_type_map,
        primary_keys=primary_keys
    )
    print(f"Creating a new table, {table_name}, with the following CREATE TABLE statement:") # noqa
    print(create_table_sql)
    return create_table_sql


def create_new_table_from_df(
    df: pd.DataFrame, table_name: str, primary_keys: Optional[list] = None
) -> None:
    """Given a df, create a new table from it.
    
    Infers the columns and dtypes from the pandas df.
    """
    try:
        create_table_statement = generate_create_table_statement_from_df(
            df=df, table_name=table_name, primary_keys=primary_keys
        )
        cursor.execute(create_table_statement)
        conn.commit()
    except Exception as e:
        print(f"Unable to create table {table_name}: {e}")
        raise


def write_df_to_database(
    df: pd.DataFrame,
    table_name: str,
    upsert: bool = False,
    primary_keys: Optional[list] = None
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
        col_to_sql_type_map = {
            col: convert_python_dtype_to_sql_type(df[col].dtype)
            for col in df.columns
        }
        df.to_sql(
            table_name,
            conn,
            if_exists="append",
            index=False,
            dtype=col_to_sql_type_map
        )
    except Exception as e:
        conn.rollback()
        print(f"Unable to write df to {table_name}: {e}")
        raise


if __name__ == "__main__":
    cursor.execute(
        "CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);"
    )
    cursor.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))
