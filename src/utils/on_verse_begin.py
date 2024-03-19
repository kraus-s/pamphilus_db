from utils.constants import *
from utils import menota_parser
from utils.menota_parser import ParallelizedNorseDoc
from collections import Counter
import pandas as pd

def main():
    pamph_doc = menota_parser.get_parallelized_text(PSDG47)

    types = []
    words = []
    types_all = []
    words_all = []
    types_anverse = []
    words_anverse = []
    types_abverse = []
    words_abverse = []

    for verse in pamph_doc.verses:
        current_type = verse.tokens[0].msa
        if len(current_type) > 3:
            msa_parts = current_type.split()
            current_type = msa_parts[0]
        if "," not in verse.vno and "a" not in verse.vno:
            
            types.append(current_type)
            words.append(verse.tokens[0].lemma)
            verse_number = int(verse.vno)
            if verse_number % 2 == 0:
                types_abverse.append(current_type)
                words_abverse.append(verse.tokens[0].lemma)
            else:
                types_anverse.append(current_type)
                words_anverse.append(verse.tokens[0].lemma)
        
        types_all.append(current_type)
        words_all.append(verse.tokens[0].lemma)
    
    results_types_all = dict(Counter(types_all))
    results_words_all = dict(Counter(words_all))
    results_types = dict(Counter(types))
    results_words = dict(Counter(words))
    results_types_anverse = dict(Counter(types_anverse))
    results_words_anverse = dict(Counter(words_anverse))
    results_types_abverse = dict(Counter(types_abverse))
    results_words_abverse = dict(Counter(words_abverse))

    res_df_types = pd.DataFrame(results_types.items(), columns=["Type", "Count"])
    res_df_words = pd.DataFrame(results_words.items(), columns=["Word", "Count"])
    res_df_types_all = pd.DataFrame(results_types_all.items(), columns=["Type", "Count"])
    res_df_words_all = pd.DataFrame(results_words_all.items(), columns=["Word", "Count"])
    res_df_types_anverse = pd.DataFrame(results_types_anverse.items(), columns=["Type", "Count"])
    res_df_words_anverse = pd.DataFrame(results_words_anverse.items(), columns=["Word", "Count"])
    res_df_types_abverse = pd.DataFrame(results_types_abverse.items(), columns=["Type", "Count"])
    res_df_words_abverse = pd.DataFrame(results_words_abverse.items(), columns=["Word", "Count"])

    res_df_types.to_csv("data/export/types_clean.csv", index=False)
    res_df_words.to_csv("data/export/words_clean.csv", index=False)
    res_df_types_all.to_csv("data/export/types_all.csv", index=False)
    res_df_words_all.to_csv("data/export/words_all.csv", index=False)
    res_df_types_anverse.to_csv("data/export/types_anverse.csv", index=False)
    res_df_words_anverse.to_csv("data/export/words_anverse.csv", index=False)
    res_df_types_abverse.to_csv("data/export/types_abverse.csv", index=False)
    res_df_words_abverse.to_csv("data/export/words_abverse.csv", index=False)

if __name__ == "__main__":
    main()