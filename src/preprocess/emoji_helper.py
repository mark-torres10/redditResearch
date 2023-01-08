import pickle

from emoji.unicode_codes.data_dict import EMOJI_DATA

TOP_EMOJIS = ['😂','🤣','😡','🖕','😹','🙏','👎','🌊','🙄','🤔']

emoji_name_to_unicode_map = {
    EMOJI_DATA[unicode_key]["en"]: unicode_key
    for unicode_key in EMOJI_DATA
}

emoji_unicode_to_name_map = {
    unicode_key: EMOJI_DATA[unicode_key]["en"]
    for unicode_key in EMOJI_DATA
}

if __name__ == '__main__':
    with open("emoji_name_to_unicode_map.pickle", "wb") as file:
        pickle.dump(
            emoji_name_to_unicode_map, file, protocol=pickle.HIGHEST_PROTOCOL
        )
