tmp_table_name = "tmp_messaged_users"
table_name = "user_to_message_status"
table_fields = [
    "user_id", "message_status", "last_update_timestamp", "last_update_step",
    "phase", "comment_id", "comment_text", "dm_text", "author_screen_name"
]
payload_required_fields = [
    "author_screen_name", "user_id", "comment_id", "comment_text",
    "message_subject", "message_body", "phase"
]
MAX_NUMBER_RETRIES = 3
