import os

from lib.helper import CODE_DIR, CURRENT_TIME_STR

ANALYSIS_ROOT_DIR = os.path.join(CODE_DIR, "analysis")
AUTHOR_PHASE_SCORES_DIR = os.path.join(
    ANALYSIS_ROOT_DIR, "author_phase_scores"
)
AUTHOR_PHASE_SCORES_FILENAME = f"author_phase_scores_{CURRENT_TIME_STR}.csv"
AUTHOR_PHASE_SCORES_FILEPATH = os.path.join(
    AUTHOR_PHASE_SCORES_DIR, AUTHOR_PHASE_SCORES_FILENAME
)
