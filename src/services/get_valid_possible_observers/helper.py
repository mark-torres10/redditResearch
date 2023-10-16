import datetime

from data.helper import dump_df_to_csv
from lib.db.sql.helper import load_table_as_df, write_df_to_database
from services.message_users.helper import table_name


def get_valid_possible_observers() -> None:
    """Get users who have not been DMed yet and have not been marked as
    pending_message, then mark them as pending_message and assign them to the
    observer phase. Then, upsert the `user_to_message_status` table with the
    new assignments.
    """
    unavailable_message_statuses = [
        "messaged_successfully", "message_failed_dm_forbidden",
        "message_failed_rate_limit",
    ]
    unavailable_message_statuses_str = ', '.join(unavailable_message_statuses)
    where_filter = f"""
        WHERE message_status NOT IN ({unavailable_message_statuses_str})
    """
    user_to_message_status_df = load_table_as_df(
        table_name=table_name,
        select_fields=["*"],
        where_filter=where_filter
    )
    user_to_message_status_df["last_update_timestamp"] = (
        datetime.datetime.utcnow().isoformat()
    )
    user_to_message_status_df["last_update_step"] = "get_valid_possible_observers" # noqa
    user_to_message_status_df["phase"] = "observer"
    user_to_message_status_df["message_status"] = "pending_message"

    print(f"Assigned {len(user_to_message_status_df)} users to observer phase.") # noqa
    dump_df_to_csv(
        df=user_to_message_status_df,
        table_name=table_name
    )
    write_df_to_database(
        df=user_to_message_status_df, table_name=table_name, upsert=True
    )
    print("Completed getting valid possible observers.")
