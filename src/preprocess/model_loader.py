import pickle

def get_nb_vectorizer(nb_model, nb_vectorizer):
    try:
        nb_model = pickle.load(open(nb_model, 'rb'))
    except:
        nb_model = pickle.load(open(nb_model, 'rb'), encoding='latin1')
    try:
        nb_vectorizer = pickle.load(open(nb_vectorizer, 'rb'))
    except:
        nb_vectorizer = pickle.load(open(nb_vectorizer, 'rb'), encoding='latin1')
    return nb_model, nb_vectorizer