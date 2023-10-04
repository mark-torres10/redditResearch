"""Helper utilities for interacting with Postgres."""
from dotenv import load_dotenv
import os
from typing import Optional

import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

from lib.db.sql.tables import TABLE_TO_KEYS_MAP

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

# https://saturncloud.io/blog/writing-dataframes-to-a-postgres-database-using-psycopg2/
engine = create_engine(
    f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"
)


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


def generate_single_foreign_key_statement(foreign_key: dict) -> str:
    return f"""
        FOREIGN KEY {foreign_key['key']}
        REFERENCES {foreign_key['reference_table']} ({foreign_key['reference_table_key']})
        ON DELETE {foreign_key['on_delete']}
    """

def create_foreign_key_statement(foreign_keys: list[dict]) -> str:
    if len(foreign_keys) == 0:
        return ""

    foreign_key_statements = [
        generate_single_foreign_key_statement(foreign_key)
        for foreign_key in foreign_keys
    ]
    foreign_key_str = ',\n'.join(foreign_key_statements)
    return f",{foreign_key_str}"


# https://www.postgresqltutorial.com/postgresql-python/create-tables/
def generate_create_table_statement(
    table_name: str, field_to_sql_type_map: dict
) -> str:
    primary_keys: list[str] = TABLE_TO_KEYS_MAP[table_name]["primary_keys"]
    foreign_keys: list[dict] = TABLE_TO_KEYS_MAP[table_name]["foreign_keys"]
    fields_list = [
        f"{field_name} {field_type} NOT NULL"
        for field_name, field_type in field_to_sql_type_map.items()
    ]
    fields_query = ',\n'.join(fields_list)
    primary_keys = f",PRIMARY KEY ({', '.join(primary_keys)})"
    foreign_keys = create_foreign_key_statement(foreign_keys)
    create_table_sql = f"""CREATE TABLE {table_name} (
        {fields_query},
        {primary_keys},
        {foreign_keys}
    )
    """
    return create_table_sql
   

def generate_create_table_statement_from_df(
    df: pd.DataFrame, table_name: str
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
        field_to_sql_type_map=col_to_sql_type_map
    )
    print(f"Creating a new table, {table_name}, with the following CREATE TABLE statement:") # noqa
    print(create_table_sql)
    return create_table_sql


def create_new_table_from_df(
    df: pd.DataFrame, table_name: str
) -> None:
    """Given a df, create a new table from it.
    
    Infers the columns and dtypes from the pandas df.
    """
    try:
        create_table_statement = generate_create_table_statement_from_df(
            df=df, table_name=table_name
        )
        cursor.execute(create_table_statement)
        conn.commit()
        print(f"Table {table_name} created successfully.")
    except Exception as e:
        print(f"Unable to create table {table_name}: {e}")
        raise


# TODO: implement upsert
def write_df_to_database(
    df: pd.DataFrame,
    table_name: str,
    upsert: bool = False
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
            print(f"Table {table_name} doesn't exist. Creating now...")
            create_new_table_from_df(df=df, table_name=table_name)
        col_to_sql_type_map = {
            col: convert_python_dtype_to_sql_type(df[col].dtype)
            for col in df.columns
        }
        df.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False
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
