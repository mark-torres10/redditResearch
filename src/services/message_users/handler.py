"""Manages fan-out of messaging users, for both author and observer phases.

Takes as input the users to message as well as what to message them. Returns
list of payloads for messaging each individual user.
"""
from services.message_users.helper import handle_message_users


def main(event: dict, context: dict) -> None:
    payloads = event["user_message_payloads"]
    phase = event["phase"]
    handle_message_users(payloads, phase)
