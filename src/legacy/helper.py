import os

LEGACY_ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
LEGACY_SYNC_DATA_DIR = os.path.join(LEGACY_ROOT_DIR, "sync")
LEGACY_AUTHOR_PHASE_DATA_DIR = os.path.join(LEGACY_ROOT_DIR, "message")
LEGACY_OBSERVER_PHASE_DATA_DIR = os.path.join(
    LEGACY_ROOT_DIR, "get_responses", "messaged_observers"
)
LEGACY_MESSAGES_RECEIVED_DIR = os.path.join(LEGACY_ROOT_DIR, "get_responses")
