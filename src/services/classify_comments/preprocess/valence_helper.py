import collections
from typing import Dict

from keras.models import Model
import pandas as pd


def get_valance_array(model: Model) -> Dict:
    df = pd.read_csv(model)
    words = df.Word.values
    arousal_mean = df["A.Mean.Sum"].values
    arousal_sd = df["A.SD.Sum"].values
    val_ar: Dict = {}
    val_ar = collections.defaultdict(
        lambda: {"arousal": {"mean": 0, "sd": 0}}, val_ar
    )  # noqa
    for ix, w in enumerate(words):
        val_ar[w]["arousal"]["mean"] = arousal_mean[ix]
        val_ar[w]["arousal"]["sd"] = arousal_sd[ix]
    del df, words, arousal_mean, arousal_sd
    return val_ar


#valence_arousal = os.path.join(ROOT_DIR, "model_files/valence_arousal.csv")

#valence_array = get_valance_array(valence_arousal)