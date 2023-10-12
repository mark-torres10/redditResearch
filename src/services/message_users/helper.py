import datetime

import pandas as pd
from praw.exceptions import RedditAPIException

from data.helper import (
    delete_tmp_json_data, dump_df_to_csv, dump_dict_as_tmp_json,
    load_tmp_json_data
)
from lib.db.sql.helper import load_table_as_df, write_df_to_database
from services.message_single_user.handler import main as message_single_user
from services.message_single_user.helper import catch_rate_limit_and_sleep


MAX_NUMBER_RETRIES = 3
# temporary holding table/path that will exist during the runtime of the
# message code. At end of service, the `user_to_message_status` table will be
# updated instead.
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
    num_payloads_removed = len(payloads) - len(filtered_payloads)
    if num_payloads_removed > 0:
        print(f"Removing {num_payloads_removed} possible DMs, since they would be to users who don't allow DMs.") # noqa
    return filtered_payloads


def dedupe_payloads(payloads: list[dict]) -> list[dict]:
    """Dedupes payloads by comment and user ID.
    
    Theoretically this shouldn't be necessary, but to avoid the possibility of
    it happening, we do filtering here. This guarantees that we don't
    accidentally double-DM a user during a given run.
    """
    seen_user_ids = set()
    seen_comment_ids = set()
    duplicate_payloads_count = 0

    deduped_payloads = []

    for payload in payloads:
        user_id = payload["user_id"]
        comment_id = payload["comment_id"]
        if (
            user_id not in seen_user_ids
            and comment_id not in seen_comment_ids
        ):
            deduped_payloads.append(payload)
            seen_user_ids.add(user_id)
            seen_comment_ids.add(comment_id)
        else:
            print("Duplicate payload spotted. Removing...")
            duplicate_payloads_count += 1

    if duplicate_payloads_count:
        print(f"Filtered out {duplicate_payloads_count} duplicate payloads.")

    return deduped_payloads


def preprocess_payloads(payloads: list[dict]) -> list[dict]:
    """Performs any necessary preprocessing and checks on the list of DMs to
    send."""
    filtered_payloads = filter_payloads_by_valid_users_to_dm(payloads)
    deduped_payloads = dedupe_payloads(filtered_payloads)
    return deduped_payloads

def is_valid_payload(payload: dict) -> bool:
    return set(payload.keys()) == set(payload_required_fields)


def message_users(
    payloads: list[dict]
) -> tuple[list[dict], list[dict], list[dict]]:
    successful_messages = []
    messages_to_retry = []
    failed_messages = []
    context = {}
    for payload in payloads:
        if not is_valid_payload(payload):
            raise ValueError(f"Invalid payload (fields are not correct): {payload}") # noqa
        status = message_single_user(payload, context)
        tmp_filename = f"{payload['user_id']}_{payload['comment_id']}.json" # noqa
        if status == 0:
            successful_messages.append(payload)
            # write successful DMs to a temporary table, so that in case the
            # messaging service fails at any point, there will be a record of
            # the DMs that have been successfully sent.
            updated_payload = {
                **payload, **{"message_status": "messaged_successfully"}
            }
            dump_dict_as_tmp_json(
                data=updated_payload,
                table_name=tmp_table_name,
                filename=tmp_filename
            )
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
                updated_payload = {
                    **payload,
                    **{"message_status": "message_failed_dm_forbidden"}
                }
                dump_dict_as_tmp_json(
                    data=updated_payload,
                    table_name=tmp_table_name,
                    filename=tmp_filename
                )

    return (successful_messages, messages_to_retry, failed_messages)


def add_cached_payloads_to_session(
    payloads: list[dict],
    cached_payloads: list[dict],
    successful_messages: list[dict],
    messages_to_retry: list[dict],
    failed_messages: list[dict]
) -> tuple[
    list[dict], list[dict], list[dict], list[dict]
]:
    """Add cached payloads to the current DMing session.
    
    We don't want to re-send DMs that we sent in a prior session. The only
    payloads that would be cached are those from DMs that we sent but for some
    reason the session was interrupted and we couldn't finish it. In that case,
    this will let us continue a session without repeating DMs that we've sent
    in the past, while counting the previously sent cached DMs to our count of
    successful/failed DMs.
    """
    cached_successful_messages: list[dict] = []
    cached_messages_to_retry: list[dict] = []
    cached_failed_messages: list[dict] = []

    # we use the combination of user id + comment id to map the cached payloads
    # against the current payloads. For some reason a direct search of the
    # dict object doesn't work, but doing it like this also removes duplicate
    # DMs anyways.
    map_cache_key_to_cache_data = {
        (cache_payload["user_id"], cache_payload["comment_id"]) : cache_payload
        for cache_payload in cached_payloads
    }

    map_key_to_data = {
        (payload["user_id"], payload["comment_id"]): payload
        for payload in payloads
    }

    for cache_key, cached_payload in map_cache_key_to_cache_data.items():
        if cache_key in map_key_to_data:
            map_key_to_data.pop(cache_key)
        if cached_payload["message_status"] == "messaged_successfully":
            cached_successful_messages.append(cached_payload)
        elif cached_payload["message_status"] == "message_failed_dm_forbidden":
            cached_failed_messages.append(cached_payload)

    successful_messages.extend(cached_successful_messages)
    messages_to_retry.extend(cached_messages_to_retry)
    failed_messages.extend(cached_failed_messages)
    payloads = [item for item in map_key_to_data.values()]

    return (payloads, successful_messages, messages_to_retry, failed_messages)


def handle_message_users(payloads: list[dict], phase: str) -> None:
    if not payloads:
        print("No users to message. Exiting...")
        return
    if phase not in ["author", "observer"]:
        raise ValueError(f"Invalid phase: {phase}")

    successful_messages: list[dict] = []
    messages_to_retry: list[dict] = []
    failed_messages: list[dict] = []

    payloads = preprocess_payloads(payloads)
    cached_payloads = load_tmp_json_data(table_name=tmp_table_name)
    payloads, successful_messages, messages_to_retry, failed_messages = (
        add_cached_payloads_to_session(
            payloads=payloads,
            cached_payloads=cached_payloads,
            successful_messages=successful_messages,
            messages_to_retry=messages_to_retry,
            failed_messages=failed_messages
        )
    )

    if payloads:
        successful_messages, messages_to_retry, failed_messages = (
            message_users(payloads)
        )
    else:
        print("No more DMs to send, after filtering out cached DMs.")

    print('-' * 10)
    print(f"After initial DM attempt...")
    print(f"Number of successful DMs: {len(successful_messages)}")
    print(f"Number of DMs to retry: {len(messages_to_retry)}")
    print(f"Number of failed DMs: {len(failed_messages)}")
    print('-' * 10)

    number_retries = 0

    while len(messages_to_retry) > 0 and number_retries < MAX_NUMBER_RETRIES:
        print(f"Retrying {len(messages_to_retry)} messages...")
        number_retries += 1
        retry_successful_messages, more_messages_to_retry, retry_failed_messages = ( # noqa
            message_users(messages_to_retry)
        )
        successful_messages.extend(retry_successful_messages)
        failed_messages.extend(retry_failed_messages)
        messages_to_retry = more_messages_to_retry
        print(f"After retry {number_retries}...")
        print(f"Number of new successful DMs: {len(retry_successful_messages)}") # noqa
        print(f"Number of total successful DMs: {len(successful_messages)}")
        print(f"Number of new failed DMs: {len(retry_failed_messages)}")
        print(f"Number of total failed DMs: {len(failed_messages)}")
        print(f"Number of DMs to retry: {len(messages_to_retry)}")
        print('-' * 10)

    print('-' * 20)
    print("Finished messaging service...")
    print(f"Total number of original DMs to send: {len(payloads)}")
    print(f"Total successfully sent: {len(successful_messages)}")
    print(f"Total failed (not due to rate limit issues): {len(failed_messages)}")
    print(f"Total failed after rate-limit retries: {len(messages_to_retry)}")
    print('-' * 20)

    successful_messages_df = pd.DataFrame(successful_messages)
    failed_messages_df = pd.DataFrame(failed_messages)
    rate_limit_messages_df = pd.DataFrame(messages_to_retry)

    successful_messages_df["message_status"] = "messaged_successfully"
    failed_messages_df["message_status"] = "message_failed_dm_forbidden"
    rate_limit_messages_df["message_status"] = "message_failed_rate_limit"

    user_to_message_status_df = pd.concat(
        [successful_messages_df, failed_messages_df, rate_limit_messages_df]
    )
    # NOTE: other fields should already be in the df, since they were passed
    # down in the individual payloads.
    user_to_message_status_df["last_update_timestamp"] = (
        datetime.datetime.utcnow().isoformat()
    )
    user_to_message_status_df["last_update_step"] = "message_users"
    user_to_message_status_df["phase"] = phase
    user_to_message_status_df["dm_text"] = (
        user_to_message_status_df["message_body"]
    )

    user_to_message_status_df = user_to_message_status_df[table_fields]

    # dump to .csv, upsert to DB (so that, for example, users who were not DMed
    # before will have their statuses updated.)
    dump_df_to_csv(df=user_to_message_status_df, table_name=table_name)
    write_df_to_database(
        df=user_to_message_status_df, table_name=table_name, upsert=True
    )

    # delete tmp json data (it exists solely in case there is an interruption
    # in the run, so that we don't lose data on which users we've DMed).
    delete_tmp_json_data(table_name=tmp_table_name)

    print(f"Completed messaging users for {phase} phase.")
