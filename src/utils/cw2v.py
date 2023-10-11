from typing import List
from utils.helpers.latin_parser import parse_pamphilus
from utils.helpers.menota_parser import paramenotaParse
import itertools
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
from utils.constants import *


def on_stops() -> List:
    with open("stopwords.txt", encoding='UTF-8') as file:
        stops = file.read()
        stop_list = stops.split(',')
    return stop_list


def latin_stops() -> List:
    with open("latinStops.txt") as file:
        stops = file.read()
        stop_list = stops.split(", ")
    return stop_list


def mergeonlat(latDF: pd.DataFrame, onDF: pd.DataFrame, variant: str):
    onStops = on_stops()
    latStops = latin_stops()
    latVocab = []
    onVocab = []
    outList = []
    for index, row in latDF.iterrows():
        latlemList = row['lemmata']
        outString = " ".join([str(i) for i in latlemList if i not in latStops])
        latVocab.extend(outString.split())
        onLemmings = onDF.loc[onDF['Verse'] == row['Verse'], ['Lemma', 'Variant']]
        onVar = onLemmings['Variant'].to_string(index=False)
        if variant in onVar or "a" in onVar:
            onLemmstr = onLemmings['Lemma'].to_string(index=False)
            onLemmstr = onLemmstr.replace("\n", " ")
            onLemmstr = " ".join([str(i) for i in onLemmstr.split() if i not in onStops])
            onLemList = onLemmstr.split()
            onVocab.extend(onLemList)
            outString += onLemmstr
        outList.append(outString)
    allvocab = set(itertools.chain(latVocab, onVocab))
    return outList, latVocab, onVocab, allvocab


def word_pairs_preprocess():
    latin_dict = parse_pamphilus()
    oldnorse_df = paramenotaParse()
    vocab_dict = {}
    for k, v in latin_dict.items():
        vocab_dict[k] = mergeonlat(v, oldnorse_df, k)
    return vocab_dict


def countvec(latin_vocabs: dict) -> dict[str, pd.DataFrame]:
    ms_dfs = {}
    for key, value in latin_vocabs.items():
        cv = CountVectorizer(ngram_range=(1,1))
        X = cv.fit_transform(value[0])
        XC = (X.T * X)
        XC.setdiag(0)
        names = cv.get_feature_names()
        df = pd.DataFrame(data=XC.toarray(), columns=names, index=names)
        ms_dfs[key] = df
    return ms_dfs


def count_results(val: int = 2):
    """Will take the larger coocurrence matrices and throw out any values below the val paramter.
    I.e. if any given word pair has a cooccurrence below the threshold, it gets deleted.
    Results are stored as csv files.
    
    Args:
        use_cache (bool): Default = True. Whether to use premade files or do it from scratch
        val(int): Default = 2. Lower Cutoff for coocurrences"""
    vocab_dict = word_pairs_preprocess()
    ms_dfs = countvec(vocab_dict)
    all_latin_vocab = set([x for y in vocab_dict.items() for x in y[1]])
    old_norse_vocab = set([x for y in vocab_dict.items() for x in y[2]])
    for key, value in ms_dfs.items():
        df = value.reset_index().melt(id_vars='index').query(f'value > {val}')
        df2 = pd.DataFrame(columns=['Latein', 'Altnordisch', 'Anzahl'])
        for index, row in df.iterrows():
            if row['index'] not in old_norse_vocab and row["variable"] not in all_latin_vocab: 
                df2 = df2.append({'Latein': row['index'], 'Altnordisch': row['variable'], 'Anzahl': row['value']}, ignore_index=True)
            df2.to_csv(f"{WORD_COOCURRENCES_PATH}/{key}-cooc-results.csv")


if __name__ == "__main__":
    count_results()