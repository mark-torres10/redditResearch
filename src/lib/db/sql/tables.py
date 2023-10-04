# https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-create-table/
# https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-FK
TABLE_TO_KEYS_MAP = {
    "subreddits": {
        "primary_keys": ["id"],
        "foreign_keys": []
    },
    "comments": {
        "primary_keys": ["id"],
        "foreign_keys": [
            {
                "key": "subreddit_id",
                "reference_table": "subreddits",
                "reference_table_key": "id",
                "on_delete": "CASCADE",
            },
            {
                "key": "thread_id",
                "reference_table": "threads",
                "referene_table_key": "id",
                "on_delete": "CASCADE",
            },
            {
                "key": "author_id",
                "reference_table": "users",
                "reference_table_key": "id",
                "on_delete": "CASCADE",
            }
        ]
    },
    "threads": {
        "primary_keys": ["id"],
        "foreign_keys": [
            {
                "key": "subreddit_id",
                "reference_table": "subreddits",
                "reference_table_key": "id",
                "on_delete": "CASCADE",
            }
        ]
    },
    "users": {
        "primary_keys": ["id"],
        "foreign_keys": []
    }
}

# TODO: remove the t5_ prefix from subreddit_id