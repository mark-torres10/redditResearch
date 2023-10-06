from praw.exceptions import RedditAPIException

from lib.db.sql.helper import load_table_as_df
from services.message_single_user.handler import main as message_single_user
from services.message_single_user.helper import catch_rate_limit_and_sleep


MAX_NUMBER_RETRIES = 3


def filter_payloads_by_valid_users_to_dm(payloads: list[dict]) -> list[dict]:
    """Filter payloads so that we only DM the users who we will be allowed to
    DM. These are the users who haven't blocked us and who accept DMs.
    
    This isn't a guarantee that we'll be able to send the DMs successfully to
    the user, since the `users` table won't necessarily be up-to-date (for
    example, the user could block us and our table doesn't reflect this).
    """
    select_fields = ["name"]
    where_filter = "WHERE is_blocked = FALSE AND accept_pms = TRUE"
    filtered_users_df = load_table_as_df(
        table_name="users",
        select_fields=select_fields,
        where_filter=where_filter
    )
    valid_users_to_dm = set(filtered_users_df["name"].tolist())
    filtered_payloads = [
        payload
        for payload in payloads
        if payload["author_screen_name"] in valid_users_to_dm
    ]
    return filtered_payloads


def message_users(
    payloads: list[dict]
) -> tuple[list[dict], list[dict], list[dict]]:
    successful_messages = []
    messages_to_retry = []
    failed_messages = []
    context = {}
    for payload in payloads:
        status = message_single_user(payload, context)
        if status == 0:
            successful_messages.append(payload)
        else:
            # TODO: should make sure that if I hit a rate error, that this is
            # caught and I sleep for the appropriate amount of time.
            if (
                isinstance(status, RedditAPIException)
                and status.error_type == "RATELIMIT"
            ):
                catch_rate_limit_and_sleep(status)
                messages_to_retry.append(payload)
            else:
                print(f"Error sending message: {status}")
                print("Not retrying this particular DM. Adding to failed DMs.")
                failed_messages.append(payload)

    return (successful_messages, messages_to_retry, failed_messages)
