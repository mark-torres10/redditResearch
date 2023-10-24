# https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-create-table/
# https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-FK
TABLE_TO_KEYS_MAP = {
    "subreddits": {
        "primary_keys": ["id"],
        "foreign_keys": []
    },
    "users": {
        "primary_keys": ["id"],
        "foreign_keys": []
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
    "comments": {
        "primary_keys": ["id"],
        "foreign_keys": [
            {
                "key": "subreddit_id",
                "reference_table": "subreddits",
                "reference_table_key": "id",
                "on_delete": "CASCADE",
            },
            # {
            #     "key": "parent_thread_id",
            #     "reference_table": "threads",
            #     "reference_table_key": "id",
            #     "on_delete": "CASCADE",
            # },
            {
                "key": "author_id",
                "reference_table": "users",
                "reference_table_key": "id",
                "on_delete": "CASCADE",
            }
        ]
    },
    "classified_comments": {
        "primary_keys": ["id"],
        "foreign_keys": [
            {
                "key": "id",
                "reference_table": "comments",
                "reference_table_key": "id",
                "on_delete": "CASCADE"
            }
        ]
    },
    "user_to_message_status": {
        "primary_keys": ["user_id"],
        "foreign_keys": [
            {
                "key": "user_id",
                "reference_table": "users",
                "reference_table_key": "id",
                "on_delete": "CASCADE"
            }
        ]
    },
    "messages_received": {
        "primary_keys": ["id"],
        "foreign_keys": [
            {
                "key": "author_id",
                "reference_table": "user_to_message_status",
                "reference_table_key": "user_id",
                "on_delete": "CASCADE"
            }
        ]
    },
    "annotated_messages": {
        "primary_keys": ["id"],
        "foreign_keys": [
            {
                "key": "id",
                "reference_table": "messages_received",
                "reference_table_key": "id",
                "on_delete": "CASCADE"
            }
        ]
    },
    "comments_available_to_evaluate_for_observer_phase": {
        "primary_keys": ["comment_id"],
        "foreign_keys": [
            {
                "key": "annotated_message_id",
                "reference_table": "annotated_messages",
                "reference_table_key": "id",
                "on_delete": "CASCADE"
            }
        ]
    },
    "comment_to_observer_map": {
        "primary_keys": ["comment_id", "user_id"],
        "foreign_keys": [
            {
                "key": "comment_id",
                "reference_table": "comments",
                "reference_table_key": "id",
                "on_delete": "CASCADE"
            },
            {
                "key": "user_id",
                "reference_table": "users",
                "reference_table_key": "id",
                "on_delete": "CASCADE"
            }
        ]
    }
}
