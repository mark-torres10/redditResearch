import datetime

from data.helper import dump_df_to_csv
from lib.db.sql.helper import load_table_as_df, write_df_to_database
from services.message_users.helper import table_name as message_status_table_name # noqa


def get_valid_possible_observers() -> None:
    """Get users who have not been DMed yet and have not been marked as
    pending_message, then mark them as pending_message and assign them to the
    observer phase. Then, upsert the `user_to_message_status` table with the
    new assignments.
    """
    # NOTE: we make available anyone who has not been messaged, including
    # anyone who has been marked as "pending_message" from the author phase.
    # It is possible for us to assign more people to be messaged in the author
    # phase than we want to actually DM, and we want to message many observers,
    # so we allow them to be marked for the observer phase even if they were
    # initially assigned for the other phase. We can override someone who was
    # marked as "pending_message" in the author phase and assign them to the
    # observer phase but the opposite isn't true (any users marked for the
    # observer phase will remain in the observer phase).
    # The statuses that should be made available for the observer phase are:
    # "not_messaged" and "pending_message"
    unavailable_message_statuses = [
        "messaged_successfully", "message_failed_dm_forbidden",
        "message_failed_rate_limit",
    ]
    unavailable_message_statuses_str = ', '.join(
        [f"'{message}'" for message in unavailable_message_statuses]
    )
    where_filter = f"""
        WHERE message_status NOT IN ({unavailable_message_statuses_str})
    """
    user_to_message_status_df = load_table_as_df(
        table_name=message_status_table_name,
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
        table_name=message_status_table_name
    )
    write_df_to_database(
        df=user_to_message_status_df,
        table_name=message_status_table_name,
        upsert=True
    )
    print("Completed getting valid possible observers.")
