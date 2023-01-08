from joblib import load
from typing import Tuple

from keras.models import load_model
from keras.preprocessing.sequence import pad_sequences
import pandas as pd

from preprocess.strings import obtain_string_features_dict

def load_embedding_and_tokenizer(
    model_name, tokenizer_joblib_file, threshold_acc
):
    gru_model = load_model(model_name, custom_objects={'threshold_acc': threshold_acc})
    embedding_tokenizer = load(tokenizer_joblib_file)
    return gru_model, embedding_tokenizer


def main(text: str) -> Tuple[float, int]: 
    """Take single text, return inferred outrage classification."""
    
    text_features = obtain_string_features_dict(text)
    embedding, tokenizer = load_embedding_and_tokenizer()
    
    df = pd.DataFrame(text_features)
    
    tweet_emb_processed = pad_sequences(
        tokenizer.texts_to_sequences(df['wn_lemmatize_hashtag']), 
        padding='post', maxlen=50
    )
    tweet_gru_predict  = embedding.predict(tweet_emb_processed)
    gru_prob = tweet_gru_predict.ravel()
    gru_binary = 1 if gru_prob > .51 else 0

    return gru_prob, gru_binary
    

if __name__ == "__main__":
    text = "this is an example text"
    prob, label = main(text)
    print(f"Outrage probability: {prob}\tLabel: {label}")

