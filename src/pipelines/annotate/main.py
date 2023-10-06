"""Retrieves new messages and runs annotation on those messages."""
from lib.helper import track_function_runtime
from services.annotate_messages.handler import main as annotate_messages
from services.get_received_messages.handler import main as get_received_messages # noqa


@track_function_runtime
def main() -> None:
    event = {}
    context = {}
    get_received_messages(event, context)
    annotate_messages(event, context)
    print("Completed message retrieval and annotation phase.")
