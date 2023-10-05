"""Takes as input a batch of comments and returns classifications.

Batching managed by whatever is calling this service. Can possibly
even be a batch of size 1.
"""
# NOTE: need to import the necessary models outside of
# the main function, otherwise the lambda function
# will import the models every time it is called
from services.classify_comments.helper import classify_comments

def main(event: dict, context: dict) -> int:
    classify_new_comments_only = event.get("classify_new_comments_only", True)
    classify_comments(classify_new_comments_only=classify_new_comments_only)
    return 0
