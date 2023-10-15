"""Backup the existing Postgres data.

Saves to a `lib/db/sql/backup/{timestamp}` directory. Dumps both a compressed
version of the data as well as the schemas of every existing table.
"""
import gzip
import json
import os

from lib.db.sql.helper import (
    current_file_directory, cursor, get_all_tables_in_db,
    get_table_col_to_dtype_map
)
from lib.helper import CURRENT_TIME_STR

def init_directory() -> str:
    backup_dir = os.path.join(current_file_directory, "snapshots")
    if not os.path.exists(backup_dir):
        os.mkdir(backup_dir)
    timestamp_dir = os.path.join(backup_dir, CURRENT_TIME_STR)
    if not os.path.exists(timestamp_dir):
        os.mkdir(timestamp_dir)
    return timestamp_dir


def dump_table_to_sql(
    table_name: str,
    table_fp: str,
    zipped: bool = False
) -> None:
    query = f"SELECT * FROM {table_name}"
    copy_query = f"COPY ({query}) TO STDOUT"
    if zipped:
        assert table_fp.endswith(".sql.gz")
        with gzip.open(table_fp, "wt", encoding="utf-8") as gzipped_file:
            cursor.copy_expert(copy_query, gzipped_file)
    else:
        assert table_fp.endswith(".sql")
        with open(table_fp, "w", encoding="utf-8") as f:
            cursor.copy_expert(copy_query, f)   
    print(f"Successfully dumped '{table_name}' table to {table_fp}.")


def dump_postgres_db(
    timestamp_dir: str, table_list: list[str], zipped: bool=False
) -> None:
    print(f"Dumping DB tables to {timestamp_dir}...")
    for table in table_list:
        suffix = ".sql.gz" if zipped else ".sql"
        table_fp = os.path.join(timestamp_dir, f"{table}{suffix}")
        dump_table_to_sql(
            table_name=table,
            table_fp=table_fp,
            zipped=zipped
        )
        print(f"Finished dumping {table} table to {table_fp}.")
    print(f"Finished successfully dumping tables from DB.")


def dump_postgres_db_schemas_to_file(
    timestamp_dir: str, table_list: list[str], zipped: bool=False
) -> None:
    print(f"Dumping DB schemas to {timestamp_dir}...")
    table_cols_to_dtypes_maps: list[dict] = [
        get_table_col_to_dtype_map(table_name) for table_name in table_list
    ]
    suffix = ".jsonl.gz" if zipped else ".jsonl"
    schemas_filename = f"schemas{suffix}"
    full_fp = os.path.join(timestamp_dir, schemas_filename)
    if zipped:
        with gzip.open(full_fp, "wt", encoding="utf-8") as gzipped_file:
             for item in table_cols_to_dtypes_maps:
                gzipped_file.write(json.dumps(item) + "\n")
    else:
        with open(full_fp, 'w', encoding="utf-8") as f:
            for item in table_cols_to_dtypes_maps:
                f.write(json.dumps(item) + "\n")
    print(f"Finished dumping DB schema file to {full_fp}.")


def main() -> None:
    timestamp_dir = init_directory()
    table_list: list[str] = get_all_tables_in_db()
    dump_postgres_db(
        timestamp_dir=timestamp_dir, table_list=table_list, zipped=True
    )
    dump_postgres_db_schemas_to_file(
        timestamp_dir=timestamp_dir, table_list=table_list, zipped=False
    )


if __name__ == "__main__":
    main()
