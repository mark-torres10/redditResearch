"""Helper code + functions for preprocessing."""
from collections import Counter
import csv
import os
import re
import string
from typing import Any, Dict, List, Optional, Tuple

from nltk import pos_tag
from nltk.corpus import stopwords, wordnet
from nltk.stem.snowball import SnowballStemmer
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer, word_tokenize
import numpy as np

from services.classify_comments.preprocess.emoji_helper import (
    emoji_name_to_unicode_map,
    emoji_unicode_to_name_map,
    TOP_EMOJIS,
)

PUNCTUATION_REGEX = r"[%s]" % re.escape("""!"$%&()*+,-./:;<=>?@[\]^_`{|}~""")
SHORT_LINK_REGEX = "//t.co\S+"
URL_LINK_REGEX = "http\S+\s*"
LEMMATIZER = WordNetLemmatizer()
STOPWORDS = stopwords.words("english")
MIN_WORD_LENGTH = 3
POS = ["adj", "verb", "noun", "adv", "pronoun", "wh", "other"]

"""
exp_outrage = os.path.join(
    ROOT_DIR, "model_files/expanded_outrage_dictionary_stemmed.csv"
)
"""

def char_is_emoji(char: str) -> bool:
    return char in emoji_name_to_unicode_map.values()


def string_is_emoji_name(string: str) -> bool:
    return string in emoji_name_to_unicode_map.keys()


def extract_emojis(string: str) -> str:
    """From a given string, extract all the emojis."""
    return " ".join([char for char in string if char_is_emoji(char)])


def get_hashtags_from_string(string: str) -> str:
    """Given a string of text, get all the hashtags."""
    return " ".join(
        [token.lower() for token in string.split() if token.startswith("#")]
    )


def string_has_link(string: str) -> bool:
    return bool(
        re.findall(SHORT_LINK_REGEX, string) or re.findall(URL_LINK_REGEX, string)
    )


def remove_links(string: str) -> str:
    """Remove Twitter links from the given string.

    Parameters:
        string (str): The input string to remove links from.

    Returns:
        str: The input string with Twitter links removed.
    """
    return re.sub("//t.co\S+", "", string)


def remove_urls(string: str) -> str:
    """Remove URLs from the given string.

    Parameters:
        string (str): The input string to remove URLs from.

    Returns:
        str: The input string with URLs removed.
    """
    return re.sub("http\S+\s*", "", string)


def remove_rt_and_cc(string: str) -> str:
    """Remove 'RT' and 'cc' from the given string.

    Parameters:
        string (str): The input string to remove 'RT' and 'cc' from.

    Returns:
        str: The input string with 'RT' and 'cc' removed.
    """
    return re.sub("RT|cc", "", string)


def remove_hashtags(string: str) -> str:
    """Remove hashtags from the given string.

    Parameters:
        string (str): The input string to remove hashtags from.

    Returns:
        str: The input string with hashtags removed.
    """
    return re.sub("#\S+", "", string)


def remove_mentions(string: str) -> str:
    """Remove mentions from the given string.

    Parameters:
        string (str): The input string to remove mentions from.

    Returns:
        str: The input string with mentions removed.
    """
    return re.sub("@\S+", "", string)


def remove_punctuation(string: str) -> str:
    """Remove punctuation from the given string.

    Parameters:
        string (str): The input string to remove punctuation from.

    Returns:
        str: The input string with punctuation removed.
    """
    return re.sub(PUNCTUATION_REGEX, "", string)


def remove_whitespace(string: str) -> str:
    """Remove extra whitespace from the given string.

    Parameters:
        string (str): The input string to remove extra whitespace from.

    Returns:
        str: The input string with extra whitespace removed.
    """
    return re.sub("\s+", " ", string)


def remove_stopwords_and_short_words(string: str) -> str:
    """Remove stopwords and short words from the given string.

    Parameters:
        string (str): The input string to remove stopwords and short words from.

    Returns:
        str: The input string with stopwords and short words removed.
    """
    return " ".join(
        word
        for word in string.split()
        if word.lower() not in STOPWORDS and len(word) >= MIN_WORD_LENGTH
    )


def token_postag(string: str) -> List[Tuple[str, str]]:
    tokens = word_tokenize(string)
    return pos_tag(tokens)


def get_wordnet_pos(treebank_tag: str) -> Optional[str]:
    if treebank_tag.startswith("J"):
        return wordnet.ADJ
    elif treebank_tag.startswith("V"):
        return wordnet.VERB
    elif treebank_tag.startswith("N"):
        return wordnet.NOUN
    elif treebank_tag.startswith("R"):
        return wordnet.ADV
    else:
        return None


def modify_pos(dict: Dict) -> Dict:
    result_dic: Dict = {}
    for key in dict.keys():
        if key.startswith("J"):
            if "adj" in result_dic:
                result_dic["adj"] += dict[key]
            else:
                result_dic["adj"] = dict[key]
        elif key.startswith("V"):
            if "verb" in result_dic:
                result_dic["verb"] += dict[key]
            else:
                result_dic["verb"] = dict[key]
        elif key.startswith("N"):
            if "noun" in result_dic:
                result_dic["noun"] += dict[key]
            else:
                result_dic["noun"] = dict[key]
        elif key.startswith("R"):
            if "adv" in result_dic:
                result_dic["adv"] += dict[key]
            else:
                result_dic["adv"] = dict[key]
        elif key in ["PRP", "PRP$"]:
            if "pronoun" in result_dic:
                result_dic["pronoun"] += dict[key]
            else:
                result_dic["pronoun"] = dict[key]
        elif key.startswith("W"):
            if "wh" in result_dic:
                result_dic["wh"] += dict[key]
            else:
                result_dic["wh"] = dict[key]
        else:
            if "other" in result_dic:
                result_dic["other"] += dict[key]
            else:
                result_dic["other"] = dict[key]
    return result_dic


def tokenize_stem_lemmatize_string(string: str) -> str:
    tokens_pos = token_postag(string)
    result_string = ""
    for word, tag in tokens_pos:
        wntag = get_wordnet_pos(tag)
        if wntag is None:
            result_string += LEMMATIZER.lemmatize(word.lower())
        else:
            result_string += LEMMATIZER.lemmatize(word.lower(), pos=wntag)
        result_string += " "
    return result_string


def preprocess_text(string: str) -> str:
    """Preprocess the given text by removing links, URLs, 'RT' and 'cc', hashtags, mentions,
    punctuation, extra whitespace, stopwords and short words, and stemming and lemmatizing the
    remaining words.

    Parameters:
        string (str): The input string to preprocess.

    Returns:
        str: The preprocessed string.
    """
    string = remove_links(string)
    string = remove_urls(string)
    string = remove_rt_and_cc(string)
    string = remove_hashtags(string)
    string = remove_mentions(string)
    string = remove_punctuation(string)
    string = remove_whitespace(string)
    string = remove_stopwords_and_short_words(string)
    string = tokenize_stem_lemmatize_string(string)
    return string


def psy_string_process(text: str) -> Tuple[List[str], int]:
    keep = set(["!", "?"])
    stop = set(stopwords.words("english"))
    remove = set([x for x in list(string.punctuation) if x not in keep])
    stop.update(remove)
    stop.update(["", " ", "  "])
    del keep, remove

    stemmer = SnowballStemmer("english")
    tokenizer = TweetTokenizer()
    text_tokenized = tokenizer.tokenize(text)
    n = len(text_tokenized)
    try:
        text_tokenized = [
            str(y.encode("utf-8"), errors="ignore") for y in text_tokenized
        ]
        stemmed = [stemmer.stem(y) for y in text_tokenized]
    except:
        stemmed = [stemmer.stem(y) for y in text_tokenized]
        stemmed = [d for d in stemmed if d not in stop]
    return stemmed, n


def get_arousal(val_ar: Dict, stemmed: List[str], n: int) -> float:
    text_arr = np.zeros(n)
    mean = np.zeros(n)
    sd = np.zeros(n)
    comp = set(stemmed) & set([*val_ar])
    for ix, w in enumerate(stemmed):
        if w in comp:
            text_arr[ix] = 1.0
            mean[ix] = val_ar[w]["arousal"]["mean"]
            sd[ix] = val_ar[w]["arousal"]["sd"]
    total_sd = np.sum(sd) * text_arr
    with np.errstate(divide="ignore", invalid="ignore"):
        sd_ratio = total_sd / sd
        sd_ratio[sd == 0] = 0
    sd_weight = sd_ratio / np.sum(sd_ratio)
    if np.isnan(
        np.sum(mean * sd_weight)
    ):  # Fix code here to make sure there is no NaN, original code does not work
        arousal_score = 0
    else:
        arousal_score = np.sum(mean * sd_weight)
    return arousal_score


# unsure what the model and vectorizer types are.
"""
def get_sentiment(nb_model: Any, nb_vectorizer: Any, stemmed: List[str]) -> float:
    if not stemmed:
        return 0
    vectorized = nb_vectorizer.transform(stemmed)
    sentiment_score = np.average(1 - nb_model.predict_proba(vectorized)[:, 1])
    return sentiment_score
"""


def get_expanded_outrage(stemmed: List[str]) -> int:
    # with open(exp_outrage, 'r') as f:
    #    exp_outrage_list = list(csv.reader(f))[0]

    # expanded_outrage_count = 0
    # for stem in stemmed:
    #    expanded_outrage_count += len(set([stem]) & set(exp_outrage_list))

    # return expanded_outrage_count
    return 0


def obtain_string_features_dict(string: str) -> Dict:
    string_features_map: Dict[str, Any] = {}
    string_features_map["text"] = str(string)
    string_features_map["hashtag"] = get_hashtags_from_string(string)
    string_features_map["wn_lemmatize"] = preprocess_text(string)
    string_features_map["wn_lemmatize_hashtag"] = " ".join(
        [
            x
            for x in string_features_map["wn_lemmatize"].split()
            + string_features_map["hashtag"].split()
            if x
        ]
    )
    (
        string_features_map["psy_stemmed"],
        string_features_map["len_tokenize"],
    ) = psy_string_process(string)
    """
    string_features_map["get_arousal"] = get_arousal(
        VALENCE_ARRAY,
        string_features_map["psy_stemmed"],
        string_features_map["len_tokenize"],
    )
    """
    """
    string_features_map["get_sentiment"] = get_sentiment(
        NB_MODEL, NB_VECTORIZER, string_features_map["psy_stemmed"]
    )
    """
    string_features_map["get_expanded_outrage"] = get_expanded_outrage(
        string_features_map["psy_stemmed"]
    )

    string_features_map["emojis_list"] = extract_emojis(string)
    string_features_map["raw_len"] = len(string)
    string_features_map["has_hashtag"] = 1 if "#" in string else 0
    string_features_map["has_mention"] = 1 if "@" in string else 0
    string_features_map["has_link"] = 1 if string_has_link(string) else 0
    string_features_map["count_emoji"] = sum(
        [1 if char_is_emoji(char) else 0 for char in string]
    )
    string_features_map["len_processed"] = len(string_features_map["wn_lemmatize"])

    # get top emojis and extract them into features
    for emoji in TOP_EMOJIS:
        name = emoji_unicode_to_name_map[emoji]
        string_features_map[name] = [
            1 if emoji in string_features_map["emojis_list"] else 0
        ]

    string_features_map["pos_count"] = modify_pos(
        Counter(elem[1] for elem in token_postag(string_features_map["wn_lemmatize"]))
    )

    for pos_tag in POS:
        string_features_map[pos_tag] = (
            0
            if pos_tag not in string_features_map["pos_count"]
            else string_features_map["pos_count"][pos_tag]
        )

    return string_features_map


if __name__ == "__main__":
    obtain_string_features_dict("foo")
