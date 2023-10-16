"""Classifies comments."""
from lib.helper import track_function_runtime
from services.classify_comments.handler import main as classify_comments


@track_function_runtime
def main() -> None:
    event = {"classify_new_comments_only": True, "num_comments_to_classify": 2000}
    context = {}
    classify_comments(event, context)
    print("Completed classification service.")


if __name__ == "__main__":
    main()
