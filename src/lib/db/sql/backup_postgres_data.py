"""Backup the existing Postgres data.

Saves to a `lib/db/sql/backup/{timestamp}` directory. Dumps both a compressed
version of the data as well as the schemas of every existing table.
"""
import os

from lib.db.sql.helper import current_file_directory
from lib.helper import CURRENT_TIME_STR

def init_directory() -> None:
    backup_dir = os.path.join(current_file_directory, "backup")
    if not os.path.exists(backup_dir):
        os.mkdir(backup_dir)
    timestamp_dir = os.path.join(backup_dir, CURRENT_TIME_STR)
    if not os.path.exists(timestamp_dir):
        os.mkdir(timestamp_dir)


def dump_postgres_db_to_compressed_file() -> None:
    pass


def dump_postgres_db_schemas_to_file() -> None:
    pass


def main() -> None:
    init_directory()
    dump_postgres_db_to_compressed_file()
    dump_postgres_db_schemas_to_file()


if __name__ == "__main__":
    main()
