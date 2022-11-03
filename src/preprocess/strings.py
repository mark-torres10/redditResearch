"""Helper code + functions for preprocessing."""
import re

import emoji
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import stopwords

EMOJI_UNICODES = set(emoji.unicode_codes.UNICODE_EMOJI.values())
TOP_EMOJIS = ['😂','🤣','😡','🖕','😹','🙏','👎','🌊','🙄','🤔']
PUNCTUATION_REGEX = r'[%s]' % re.escape("""!"$%&()*+,-./:;<=>?@[\]^_`{|}~""")
SHORT_LINK_REGEX = '//t.co\S+'
URL_LINK_REGEX = 'http\S+\s*'
LEMMATIZER = WordNetLemmatizer()
STOPWORDS = stopwords.words("english")

def char_is_emoji(char: str) -> bool:
    return char in emoji.UNICODE_EMOJI

def string_is_emoji_name(string: str) -> bool:
    return string in EMOJI_UNICODES

def extract_emojis_from_string(string: str) -> str:
    """From a given string, extract all the emojis."""
    return ' '.join([char for char in string if char in emoji.UNICODE_EMOJI])

def get_hashtags_from_string(string :str) -> str:
    """Given a string of text, get all the hashtags."""
    return " ".join([
        token.lower()
        for token in string.split()
        if token.startswith('#')
    ])


def string_has_link(string: str) -> bool:
    return bool(
        re.findall(SHORT_LINK_REGEX, string)
        or re.findall(URL_LINK_REGEX, string)
    )
    

def remove_punctuation_from_string(string: str) -> str:
    return re.sub(PUNCTUATION_REGEX, '', string)


def remove_stopwords(string: str) -> str:
    pass


def preprocess_string(string: str) -> str:
    pass