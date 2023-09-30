"""Helper utilities for interacting with Postgres."""
from dotenv import load_dotenv
from pathlib import Path
import os

import psycopg2

load_dotenv(Path("../../../../.env"))

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


if __name__ == "__main__":
    cursor.execute(
        "CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);"
    )
    cursor.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))
