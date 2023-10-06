import pandas as pd

from data.helper import dump_df_to_csv
from lib.db.sql.helper import (
    check_if_table_exists, load_table_as_df, write_df_to_database
)

table_name = "messages_received"


def get_received_messages() -> None:
    # check table of DMs already received, filter out DMs received
    # by that table.
    # NOTE: when this table exists, can get the most recent timestamp
    # and then get only the DMs that are more recent than that...
    messages_received_table_exists = check_if_table_exists(table_name)

    # get DMs received.
    dms_received = pd.DataFrame()

    # join those DMs against the `user_to_message_status` table to get hydrated
    # info. Add the DM and the timestamp that this script runs, and add that to
    # the hydrated info.
    hydrated_dms_received = pd.DataFrame()

    # write to DB.
    dump_df_to_csv(df=hydrated_dms_received, table_name=table_name)
    write_df_to_database(df=hydrated_dms_received, table_name=table_name)

    print("Completed getting received messages.")
