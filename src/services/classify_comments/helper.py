from data.helper import dump_df_to_csv
from lib.db.sql.helper import (
    check_if_table_exists, load_table_as_df, write_df_to_database
)
from services.classify_comments.inference import (
    classify_text, load_default_embedding_and_tokenizer
)

table_name = "classified_comments"
subset_columns = [
    "id", "author_screen_name", "author_id", "body", "permalink",
    "created_utc", "subreddit_name_prefixed"
]
embedding, tokenizer = load_default_embedding_and_tokenizer()


def classify_comments(classify_new_comments_only: bool = True) -> None:
    # load comments
    select_fields = ["*"]
    where_filter = ""
    comments_df = load_table_as_df(
        table_name="comments",
        select_fields=select_fields,
        where_filter=where_filter
    )

    # load previously classified comments
    if classify_new_comments_only:
        table_exists = check_if_table_exists(table_name)
        if table_exists:
            previous_ids_fields = ["id"]
            previous_ids_where_filter = (
                "WHERE is_classified = FALSE"
                if classify_new_comments_only else ""
            )
            previous_ids_df = load_table_as_df(
                table_name="classified_comments",
                select_fields=previous_ids_fields,
                where_filter=previous_ids_where_filter
            )
            previous_ids = previous_ids_df["id"].tolist()
            comments_df = comments_df[~comments_df["id"].isin(previous_ids)]
            if comments_df.shape[0] == 0:
                print("No new comments to classify...")
                return

    # get only the subset of relevant columns from comments df that we want in
    # the table of classified comments. These are the information that we need
    # to (1) uniquely identify the comment + author and (2) populate the DM
    # that we are going to send.
    comments_df = comments_df[subset_columns]

    print(f"Number of comments to classify: {comments_df.shape[0]}")

    # classify
    texts_to_classify = comments_df["body"].tolist()
    
    probs: list[float] = []
    labels: list[int] = []

    for idx, text in enumerate(texts_to_classify):
        if idx % 50 == 0 and idx != 0:
            print(f"Classifying comment {idx} of {len(texts_to_classify)}...")
        prob, label = classify_text(text, embedding, tokenizer)
        probs.append(prob[0]) # returned as n=1 np.array, so need to extract
        labels.append(label)
    
    comments_df["prob"] = probs
    comments_df["label"] = labels
    comments_df["is_classified"] = True

    # write to CSV, upload to DB
    dump_df_to_csv(
        df=comments_df, table_name=table_name
    )
    write_df_to_database(
        df=comments_df, table_name=table_name, rebuild_table=True
    )
