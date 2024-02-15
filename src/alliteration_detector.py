from utils.constants import *
import pickle
from utils.menota_parser import ParallelizedNorseDoc
import os
from utils import menota_parser
import re
from markdown import markdown

# Meh...

def load_data() -> ParallelizedNorseDoc:
    if not os.path.exists(OLD_NORSE_PICKLE):
        pamph_dg47 = menota_parser.get_parallelized_text(PSDG47)
        pickle.dump(pamph_dg47, open(OLD_NORSE_PICKLE, "wb"))
    else:
        pamph_dg47 = pickle.load(open(OLD_NORSE_PICKLE, "rb"))
    return pamph_dg47


def detect_alliteration(text: str) -> str:
    words = text.split()
    alliteration = []

    for word in words:
        alliterate_char = word[0]
        alliterates = False
        for word_to_check in words:
            if word_to_check[0] == alliterate_char:
                alliterates = True
        if alliterates:
            alliteration.append(f"**{word[0]}**{word[1:]}")
        else:
            alliteration.append(word)
    return markdown(" ".join(alliteration))
      

if __name__ == "__main__":
    pamph_dg47 = load_data()
    verses = pamph_dg47.verses
    for index, verse in enumerate(verses):
        tokens = verse.tokens
        tokens.extend(verses[index + 1].tokens)
        text = " ".join([token.normalized for token in tokens])
        alliteration = detect_alliteration(text)
        print(alliteration)