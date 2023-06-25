import os

from lib.helper import CODE_DIR

from message.helper import AUTHOR_DM_SUBJECT_LINE, OBSERVER_DM_SUBJECT_LINE

RESPONSES_ROOT_PATH = os.path.join(CODE_DIR, "get_responses")

RESEARCH_PHASE_TO_SUBJECT_LINE_MAP = {
    "messaging": AUTHOR_DM_SUBJECT_LINE,
    "observer": OBSERVER_DM_SUBJECT_LINE
}

NUMBER_REGEX_PATTERN = r'\d+|one|two|three|four|five|six|seven|eight|nine|zero'

VALIDATED_RESPONSES_FILENAMES = "validated_responses.csv"