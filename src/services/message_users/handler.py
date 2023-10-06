"""Manages fan-out of messaging users, for both author and observer phases.

Takes as input the users to message as well as what to message them. Returns
list of payloads for messaging each individual user.
"""
from services.message_users.helper import (
    MAX_NUMBER_RETRIES, filter_payloads_by_valid_users_to_dm, message_users
)


def main(event: dict, context: dict) -> None:
    payloads = event.get("user_message_payloads", [])
    if not payloads:
        print("No users to message. Exiting...")
        return

    successful_messages: list[dict] = []
    messages_to_retry: list[dict] = []
    failed_messages: list[dict] = []

    payloads = filter_payloads_by_valid_users_to_dm(payloads)

    successful_messages, messages_to_retry, failed_messages = (
        message_users(payloads)
    )

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

    # any DMs that needed to be retried but didn't get retried should be marked
    # as failed.
    failed_messages.extend(messages_to_retry)

    print('-' * 20)
    print("Finished messaging service...")
    print(f"Total number of original DMs to send: {len(payloads)}")
    print(f"Total successfully sent: {len(successful_messages)}")
    print(f"Total failed: {len(failed_messages)}")
    print(f"Total failed after rate-limit retries: {len(messages_to_retry)}")
    print('-' * 20)
    # TODO: also is there a way to incrementally write a .csv file as well as
    # write to a DB so that we don't lose our progress?
    # NOTE: maybe write to a temp location and then at the end, move to a
    # permanent location? Or can write a 1-row .csv file or something
    # like that???
    # TODO: update user_to_message_status table. Any in successful table should
    # be marked as messaged, any in failed should be marked as message_failed.
    # then, timestamp should be updated. Maybe we should separate those that
    # fail on rate limit vs. those that fail for other reasons (e.g,. not
    # allowed to DM.) Failed should be message_failed_rate_limit, or
    # message_failed_dm_forbidden, etc.

    # TODO: Export successful results as .csv, write to DB.
    print("Completed messaging users.")
