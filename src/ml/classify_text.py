"""Given a set of messages, classify the samples."""
import os
import sys

import pandas as pd

from lib.helper import ROOT_DIR
from ml import inference

def load_posts(filepath: str) -> pd.DataFrame:
    pass


def classify_all_posts(
    posts_df: pd.DataFrame,
    post_col: str,
    classify_func: Callable,
    optional_func_args: Dict
) -> List[Tuple[float, int]]:
    return [
        classify_func(text, **optional_func_args)
        for text in posts_df[post_col]
    ]


def export_classifications(
    posts_df: pd.DataFrame,
    labels: List[Tuple[float, int]],
    full_filepath: str
) -> None:
    prob_list = [label[0] for label in labels]
    labels_list = [label[1] for label in labels]
    posts_df["probability"] = prob_list
    posts_df["label"] = labels_list
    posts_df.to_csv(full_filepath)


if __name__ == "__main__":
    load_path = sys.argv[1]
    load_filename = load_path.split('/')[-1]
    export_filepath = os.path.join(ROOT_DIR, "ml", "classifications", load_filename)
    
    embedding, tokenizer = load_default_embedding_and_tokenizer()
    posts_df = load_posts(load_path)
    labels = classify_all_posts(
        posts_df=posts_df,
        post_col="posts",
        classify_func=inference.classify_reddit_text,
        optional_func_args={"embedding": embedding, "tokenizer": tokenizer}
    )
    export_classifications(
        posts_df=posts_df,
        labels=labels,
        full_filepath=export_filepath
    )
    
