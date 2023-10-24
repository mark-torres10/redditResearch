"""Loads the backup data into a Postgres table.

By default, takes the latest snapshot.
"""
import gzip
import os

from lib.db.sql.helper import conn, cursor, current_file_directory as sql_helper_dir # noqa

snapshots_dir = os.path.join(sql_helper_dir, "snapshots")


def get_latest_snapshot() -> str:
    return max(os.listdir(snapshots_dir))


def get_compressed_files_to_dump(latest_snapshot_timestamp: str) -> list[str]:
    full_dir_path = os.path.join(
        snapshots_dir, latest_snapshot_timestamp
    )
    full_filepaths = [
        os.path.join(full_dir_path, file)
        for file in os.listdir(full_dir_path)
        if file.endswith(".sql.gz")
    ]
    return full_filepaths


def load_single_sql_dump(file_path: str) -> None:
    try:
        with gzip.open(file_path, "rb") as f:
            sql_script = f.read().decode('utf-8')
            cursor.execute(sql_script)
            conn.commit()
            print(f"Loaded {file_path} into the database successfully.")
    except Exception as e:
        print(f"Unable to load {file_path} into the database, with error {e}.")
        raise e


def load_sql_dumps_into_db(compressed_files: list[str]) -> None:
    for file_path in compressed_files:
        load_single_sql_dump(file_path)


def main() -> None:
    latest_snapshot = get_latest_snapshot()
    compressed_files = get_compressed_files_to_dump(latest_snapshot)
    load_sql_dumps_into_db(compressed_files)
    print(f"Finished loading data from {latest_snapshot} into the database.")


if __name__ == "__main__":
    main()
