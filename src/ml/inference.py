from joblib import load
import os
from typing import Tuple

from keras import backend as K
from keras.models import load_model
from keras.utils import pad_sequences

import pandas as pd

from lib.helper import ROOT_DIR
from preprocess.strings import obtain_string_features_dict

MODEL_NAME = os.path.join(ROOT_DIR, "model_files/GRU.h5")
TOKENIZER_JOBLIB_FILE = os.path.join(
    ROOT_DIR, "model_files/26k_training_data.joblib"
)
THRESHOLD = 0.70

def threshold_acc(y_true, y_pred):
    if K.backend() == 'tensorflow':
        return K.mean(
            K.equal(
                y_true, K.cast(K.greater_equal(y_pred, THRESHOLD), y_true.dtype)
            )
        )
    else:
        return K.mean(K.equal(y_true, K.greater_equal(y_pred, THRESHOLD)))


def load_embedding_and_tokenizer(
    model_name, tokenizer_joblib_file, threshold_acc
):
    gru_model = load_model(model_name, custom_objects={'threshold_acc': threshold_acc})
    embedding_tokenizer = load(tokenizer_joblib_file)
    return gru_model, embedding_tokenizer


def load_default_embedding_and_tokenizer():
    return load_embedding_and_tokenizer(
        MODEL_NAME, TOKENIZER_JOBLIB_FILE, threshold_acc
    )


def classify_text(text: str, embedding, tokenizer) -> Tuple[float, int]: 
    """Take single text, return inferred outrage classification."""
    
    text_features = obtain_string_features_dict(text)

    text_features = {key: [value] for key, value in text_features.items()}

    df = pd.DataFrame(text_features)
    
    tweet_emb_processed = pad_sequences(
        tokenizer.texts_to_sequences(df['wn_lemmatize_hashtag']), 
        padding='post', maxlen=50
    )
    tweet_gru_predict  = embedding.predict(tweet_emb_processed)
    gru_prob = tweet_gru_predict.ravel()
    gru_binary = 1 if gru_prob > .51 else 0

    return gru_prob, gru_binary


def classify_reddit_text(text: str) -> None:
    embedding, tokenizer = load_embedding_and_tokenizer(
        MODEL_NAME, TOKENIZER_JOBLIB_FILE, threshold_acc
    )
    prob, label = classify_text(text, embedding, tokenizer)
    print(f"Outrage probability: {prob}\tLabel: {label}")

if __name__ == "__main__":
    text = "I am so mad that this is happening today."
    classify_reddit_text(text)