from operator import index
from typing import List
import pickle
from sklearn.base import is_classifier
from wordsorter import latLemLoader
from wordsorter import paramenotaParse
import itertools
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
from pathlib import Path

outPath = "latmat/"
B1coP = Path("latMat/CoOc-B1.csv")
P3coP = Path("latMat/CoOc-P3.csv")
TooP = Path("latMat/CoOc-To.csv")
W1oP = Path("latMat/CoOc-W1.csv")


def on_stops() -> List:
    with open("stopwords.txt", encoding='UTF-8') as file:
        stops = file.read()
        stopList = stops.split(',')
    return stopList


def latin_stops() -> List:
    with open("latinStops.txt") as file:
        stops = file.read()
        stopList = stops.split(", ")
    return stopList


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
    B1, P3, To, W1 = latLemLoader()
    onDF = paramenotaParse()
    B1LatOn, B1vocab, pamphVocabB1, B1DGvocab = mergeonlat(B1, onDF, 'B1')
    P3LatOn, P3vocab, pamphVocabP3, P3DGvocab = mergeonlat(P3, onDF, 'P3')
    ToLatOn, Tovocab, pamphVocabTo, ToDGvocab = mergeonlat(To, onDF, 'To')
    W1LatOn, W1vocab, pamphVocabW1, W1DGvocab = mergeonlat(W1, onDF, 'W1')
    allLatVocab = set(itertools.chain(B1vocab, P3vocab, Tovocab, W1vocab))
    onVocab = set(itertools.chain(pamphVocabB1, pamphVocabP3, pamphVocabTo, pamphVocabW1))
    pickle.dump(allLatVocab, open("latPickle.p", "wb"))
    pickle.dump(onVocab, open("onPickle.p", "wb"))
    return B1LatOn, P3LatOn, ToLatOn, W1LatOn


def countvec():
    B1, P3, To, W1 = word_pairs_preprocess()
    blabla = {"B1": B1, "P3": P3, "To": To, "W1": W1}
    for key, value in blabla.items():
        cv = CountVectorizer(ngram_range=(1,1))
        X = cv.fit_transform(value)
        XC = (X.T * X)
        XC.setdiag(0)
        names = cv.get_feature_names()
        df = pd.DataFrame(data=XC.toarray(), columns=names, index=names)
        df.to_csv(f"{outPath}CoOc-{key}.csv")
        print(df)
    return


def countResults(use_cache: bool = True, val: int = 2):
    """Will take the larger coocurrence matrices and throw out any values below the val paramter.
    I.e. if any given word pair has a cooccurrence below the threshold, it gets deleted.
    Results are stored as csv files.
    
    Args:
        use_cache (bool): Default = True. Whether to use premade files or do it from scratch
        val(int): Default = 2. Lower Cutoff for coocurrences"""
    if use_cache:
        if not (B1coP.is_file() and P3coP.is_file() and TooP.is_file() and W1oP.is_file()):
            countvec()        
        else:
            B1 = pd.read_csv(B1coP, index_col=0)
            P3 = pd.read_csv(P3coP, index_col=0)
            To = pd.read_csv(TooP, index_col=0)
            W1 = pd.read_csv(W1oP, index_col=0)
    else:
        countvec()
        B1 = pd.read_csv(B1coP, index_col=0)
        P3 = pd.read_csv(P3coP, index_col=0)
        To = pd.read_csv(TooP, index_col=0)
        W1 = pd.read_csv(W1oP, index_col=0)
    blabla = {"B1": B1, "P3": P3, "To": To, "W1": W1}
    latVocab = pickle.load(open("latPickle.p", "rb"))
    onVocab = pickle.load(open("onPickle.p", "rb"))
    for key, value in blabla.items():
        df = value.reset_index().melt(id_vars='index').query(f'value > {val}')
        df2 = pd.DataFrame(columns=['Latein', 'Altnordisch', 'Anzahl'])
        for index, row in df.iterrows():
            if not row['index'] in onVocab:
                
                if not row['variable'] in latVocab:
                    df2 = df2.append({'Latein': row['index'], 'Altnordisch': row['variable'], 'Anzahl': row['value']}, ignore_index=True)
            df2.to_csv(f"latmat/{key}-cooc-results.csv")
    return


if __name__ == "__main__":
    countResults(use_cache=False)