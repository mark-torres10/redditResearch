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
    elif dtype == np.dtype("int32"):
        instance_type = "int"
    elif dtype == np.dtype("int64"):
        instance_type = "int"
    elif dtype == np.dtype("float32"):
        instance_type = "float"
    elif dtype == np.dtype("float64"):
        instance_type = "float"
    else:
        raise ValueError(f"Unknown dtype: {dtype}")

    return instance_type


def drop_table(table_name: str, cascade: bool = True) -> None:
    """Deletes a table from the database."""
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} {'CASCADE' if cascade else ''};") # noqa
        conn.commit()
        print(f"Table {table_name} deleted successfully.")
    except Exception as e:
        print(f"Unable to delete table {table_name}: {e}")
        raise


def generate_primary_key_statement(primary_keys: list[str]) -> str:
    return f"PRIMARY KEY ({', '.join(primary_keys)})"


def generate_single_foreign_key_statement(foreign_key: dict) -> str:
    return f"""
        FOREIGN KEY ({foreign_key['key']})
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
    return foreign_key_str


# https://www.postgresqltutorial.com/postgresql-python/create-tables/
def generate_create_table_statement(
    table_name: str, field_to_sql_type_map: dict
) -> str:
    primary_keys: list[str] = TABLE_TO_KEYS_MAP[table_name]["primary_keys"]
    foreign_keys: list[dict] = TABLE_TO_KEYS_MAP[table_name]["foreign_keys"]
    fields_list = [
        f"""
            {field_name} {field_type}
            {
                'NOT NULL' if field_name in primary_keys 
                or field_name in [fk['key'] for fk in foreign_keys]
                else ''
            }
        """
        for field_name, field_type in field_to_sql_type_map.items()
    ]
    fields_query = ',\n'.join(fields_list)
    primary_keys = generate_primary_key_statement(primary_keys)
    foreign_keys = create_foreign_key_statement(foreign_keys)
    create_table_sql = f"""CREATE TABLE {table_name} (
        {fields_query},
        {primary_keys}{',' if foreign_keys else ''}
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


def check_if_table_exists(table_name: str) -> bool:
    """Checks if a table exists in the database."""
    try:
        cursor.execute(
            f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='{table_name}');"
        )
        table_exists = cursor.fetchone()[0]
        return table_exists
    except Exception as e:
        print(f"Unable to check if table {table_name} exists: {e}")
        raise


def create_upsert_query_from_df(
    df: pd.DataFrame, table_name: str, upsert_keys: list[str]
) -> str:
    """Creates an upsert query from a dataframe.
    
    Upsert keys, more likely than not, should correspond to the primary keys
    of their respective tables.
    """
    upsert_query = f"""
        INSERT INTO {table_name} ({', '.join(df.columns)})
        VALUES {', '.join(['%s'] * len(df.columns))}
        ON CONFLICT ({', '.join(upsert_keys)})
        DO UPDATE SET
    """
    update_fields = [
        f"{col} = EXCLUDED.{col}"
        for col in df.columns
        if col not in upsert_keys
    ]
    upsert_query += ', '.join(update_fields)
    return upsert_query


def write_df_to_database(
    df: pd.DataFrame,
    table_name: str,
    rebuild_table: bool = False,
    upsert: bool = False
) -> None:
    """Writes a dataframe to a Postgres table.
    
    Assumes that the column names of the dataframe are the same as the column
    match that of the table schema.
    """
    try:
        # check to see if table exists. If not, create it
        if rebuild_table:
            drop_table(table_name=table_name)
            table_exists = False
        else:
            table_exists = check_if_table_exists(table_name=table_name)
        if not table_exists:
            print(f"Table {table_name} doesn't exist. Creating now...")
            create_new_table_from_df(df=df, table_name=table_name)
        if upsert:
            upsert_query = create_upsert_query_from_df(
                df=df,
                table_name=table_name,
                upsert_keys=TABLE_TO_KEYS_MAP[table_name]["primary_keys"]
            )
            # cursor.mogrify is faster than cursor.executemany()?
            # https://www.datacareer.de/blog/improve-your-psycopg2-executions-for-postgresql-in-python/
            # https://naysan.ca/2020/08/02/pandas-to-postgresql-using-psycopg2-mogrify-then-execute/
            sql_statements = [
                cursor.mogrify(upsert_query, tuple(row))
                for _, row in df.iterrows()
            ]
            print(f"Upserting {len(sql_statements)} rows into {table_name}...") # noqa
            for statement in sql_statements:
                cursor.execute(statement)
            print(f"Finished upserting {len(sql_statements)} rows into {table_name}.") # noqa
        else:
            df.to_sql(
                table_name,
                engine,
                if_exists="append",
                index=False
            )
            print(f"Finished inserting (not upserting) {len(df)} rows to {table_name}.") # noqa
    except Exception as e:
        conn.rollback()
        print(f"Unable to write df to {table_name}: {e}")
        raise


def load_query_as_df(query: str) -> pd.DataFrame:
    """Loads a query from the database into a dataframe."""
    try:
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description]) # noqa
        return df
    except Exception as e:
        print(f"Unable to load query {query}: {e}")
        raise


def load_table_as_df(
    table_name: str,
    select_fields: list[str] = ['*'],
    join_query: str = "",
    where_filter: str = "",
    order_by_clause: str = "",
    limit_clause: str = ""
) -> pd.DataFrame:
    """Loads a table from the database into a dataframe."""
    try:
        select_fields_query = ', '.join(select_fields)
        cursor.execute(f"""
            SELECT
                {select_fields_query}
            FROM {table_name}
            {join_query}
            {where_filter}
            {order_by_clause}
            {limit_clause};
        """) # noqa
        df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description]) # noqa
        return df
    except Exception as e:
        print(f"Unable to load table {table_name}: {e}")
        raise


if __name__ == "__main__":
    cursor.execute(
        "CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);"
    )
    cursor.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))
