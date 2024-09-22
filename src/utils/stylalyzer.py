from utils import menota_parser
from utils.constants import *
from utils.menota_parser import NorseDoc
import glob
import pandas as pd
import pickle
import os

"""This script is used to analyze the style of the Old Norse texts in the Menota corpus. 
It looks for specific style markers, like specific adverb suffixes etc. it is used in chapter 5.2"""

def load_data() -> list[NorseDoc]:
    if not os.path.exists(MENOTA_COMPLETE_PICKLE):
        parsed_docs_list = import_menota_data(path= OLD_NORSE_CORPUS_FILES)
        pickle.dump(parsed_docs_list, open(MENOTA_COMPLETE_PICKLE, "wb"))
    else:
        parsed_docs_list = pickle.load(open(MENOTA_COMPLETE_PICKLE, "rb"))
    return parsed_docs_list


def import_menota_data(path: str = OLD_NORSE_CORPUS_FILES) -> list[NorseDoc]:
    """
    Imports the Norse corpus from the Menota XML files
    """
    path_list = glob.glob(f"{path}*.xml")
    docs_to_parse = path_list
    entities = menota_parser.download_and_parse_entities("http://www.menota.org/menota-entities.txt")
    parsed_docs_list = [menota_parser.get_regular_text(path, entities) for path in docs_to_parse]

    return parsed_docs_list


def _basic_preprocessor(parsed_docs_list: list[NorseDoc]) -> list[NorseDoc]:
    best_list = [x for x in parsed_docs_list if x.lemmatized and x.msa and len(x.tokens) > 2000]
    print(f"Found {len(best_list)} documents with lemmatized tokens and msa")
    return best_list


def look_for_style_markers(doc: NorseDoc, pattern: tuple[str, str]) -> int:
    count = 0
    for tok in doc.tokens:
        if tok.lemma.endswith(pattern[0]) and tok.msa.startswith(pattern[1]):
            count += 1
    return count


def check_all_docs_for_style_markers(docs: list[NorseDoc]):
    markers = [("liga", "xAV"), ("ligr", "xAJ"), ("leikr", "xNC"), ("skapr", "xNC"), ("samr", "xAJ"), ("semd", "xNC")]
    for marker in markers:
        results = []
        for doc in docs:
            current_count = look_for_style_markers(doc, marker)
            all_word_count = len(doc.tokens)
            try:
                freq = current_count / all_word_count
            except ZeroDivisionError:
                import pdb; pdb.set_trace()
            freq_per_1000 = freq * 1000
            results.append((f"{doc.name}-{doc.ms}", current_count, all_word_count, freq_per_1000))
        current_results_df = pd.DataFrame(results, columns=["Text", "Count", "Total Word Count", "Frequency per 1000 words"])
        current_results_df.to_csv(f"{STYLE_MARKERS_PATH}{marker[0]}-{marker[1]}_results.csv", index=False)
        current_results_df.to_excel(f"data/export/{marker[0]}-{marker[1]}_results.xlsx", index=False)


def main():
    parsed_docs = load_data()
    best_docs = _basic_preprocessor(parsed_docs)
    check_all_docs_for_style_markers(best_docs)
    print("Done")

if __name__ == "__main__":
    main()