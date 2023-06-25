import os

from lib.helper import CODE_DIR

from message.helper import AUTHOR_DM_SUBJECT_LINE, OBSERVER_DM_SUBJECT_LINE

RESPONSES_ROOT_PATH = os.path.join(CODE_DIR, "get_responses")
VALIDATED_RESPONSES_ROOT_PATH = os.path.join(
    RESPONSES_ROOT_PATH, "validated_responses_dir"
)
RESEARCH_PHASE_TO_SUBJECT_LINE_MAP = {
    "messaging": AUTHOR_DM_SUBJECT_LINE,
    "observer": OBSERVER_DM_SUBJECT_LINE
}

NUMBER_REGEX_PATTERN = r'\d+|one|two|three|four|five|six|seven|eight|nine|zero'

VALIDATED_RESPONSES_ROOT_FILENAME = "validated_responses"
SESSION_VALIDATED_RESPONSES_FILENAME = (
    f"{VALIDATED_RESPONSES_ROOT_FILENAME}" + "_{timestamp}.csv"
)
ALL_VALIDATED_RESPONSES_FILENAME = "validated_responses_all.csv"