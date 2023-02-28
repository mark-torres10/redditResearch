import pickle
from typing import Any, Tuple


def get_nb_vectorizer(nb_model: str, nb_vectorizer: str) -> Tuple[Any, Any]: 
    """Loads Naive Bayes model and vectorizer"""
    try:
        nb_model = pickle.load(open(nb_model, "rb"))
    except:
        nb_model = pickle.load(open(nb_model, "rb"), encoding="latin1")
    try:
        nb_vectorizer = pickle.load(open(nb_vectorizer, "rb"))
    except:
        nb_vectorizer = pickle.load(open(nb_vectorizer, "rb"), encoding="latin1")
    return nb_model, nb_vectorizer
