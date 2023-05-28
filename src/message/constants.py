import os

from lib.helper import CODE_DIR

MESSAGES_ROOT_PATH = os.path.join(CODE_DIR, "message")
WHO_TO_MESSAGE_FILENAME = "posts_to_who_to_message_status.csv"
SENT_MESSAGES_FILENAME = "user_to_has_received_messages.csv"