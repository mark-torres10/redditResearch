"""Retrieves new messages and runs annotation on those messages."""
from lib.helper import track_function_runtime
from services.annotate_messages.handler import main as annotate_messages
from services.get_received_messages.handler import main as get_received_messages # noqa


@track_function_runtime
def main(annotation_only: bool=False) -> None:
    event = {}
    context = {}
    if not annotation_only:
        get_received_messages(event, context)
    annotate_messages(event, context)
    print("Completed message retrieval and annotation phase.")


if __name__ == "__main__":
    main(annotation_only=True)
