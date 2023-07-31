import itertools
from numpy import product, vectorize
from utils import latin_parser
from utils import menota_parser
from utils.constants import *
from sklearn.feature_extraction.text import TfidfVectorizer
from scipy.spatial.distance import pdist, squareform
from sklearn.metrics import pairwise_distances
import pandas as pd
import glob
from cltk.stem.latin.j_v import JVReplacer
from cltk.lemmatize.latin.backoff import BackoffLatinLemmatizer
import sqlite3
from rapidfuzz import fuzz

# Helper functions latin
# ----------------------


def get_latin_stopwords():
    with open(LATIN_STOP_WORDS, 'r') as f:
        stops = f.read()
    stops = stops.split(',')
    stops = [x.strip() for x in stops]
    return stops


def get_pamph(inFile, versify: bool = False) -> dict:
    pamph = latin_parser.parse_pamphilus(inFile)
    res = {}
    j = JVReplacer()
    if versify:
        for i in pamph:
            for ii in pamph[i].verses:
                vttl = f"{i}-{ii.vno}"
                txt = " ".join([x for x in ii.tokens])
                res[vttl] = txt              
    else:
        for i in pamph:
            doc = []
            for ii in pamph[i].verses:
                for iii in ii.tokens:
                    doc.append(iii)
            doc2 = " ".join([x for x in doc])
            res[i] = doc2
    return res


def corpus_collector_latin(lemmatize: bool = False, versify: bool = False) -> dict:
    fList = glob.glob(f"{LATIN_CORPUS_FILES}**/*.xml", recursive=True)
    corpusDIct = {}
    for f in fList:
        print(f"Now opening {f}")
        if 'pamph' in f:
            i = get_pamph(f, versify)
        else:
            i = latin_parser.parse_perseus(f, versify)
        corpusDIct.update(i)
    res = corpus_cleaner(corpusDIct, lemmatize, versify)
    return res


def corpus_cleaner(corps: dict, lemmatize: bool = False, versify: bool = False) -> dict:
    j = JVReplacer()
    res = {}
    for i in corps.keys():
        ii = corps[i].split()
        doc = []
        for iii in ii:
            iii = j.replace(iii)
            doc.append(latin_parser.enclitics(iii))
        if lemmatize:
            doc = latin_lemmatizer(doc)
        doc2 = " ".join([x for x in doc])
        res[i] = doc2
    return res


def latin_lemmatizer(tokens: list) -> list:
    lemmatizer = BackoffLatinLemmatizer()
    res = lemmatizer.lemmatize(tokens)
    return res


# Helper functions old norse
# ---------------------------


def corpus_collector_norse(level: str):
    """Params: level(str): "normalized" or "lemma"
    """
    fList = glob.glob(f"{OLD_NORSE_CORPUS_FILES}*.xml")
    res = {}
    res0 = []
    for i in fList:
        ii = menota_parser.parse(i)
        res0.append(ii)
    for iii in res0:
        if isinstance(iii, menota_parser.paradoc):
            tokens = []
            for x in iii.verses:
                if level == 'normalized':
                    toks = [y.normalized for y in x.tokens]
                elif level == 'lemma':
                    toks = [y.lemma for y in x.tokens]
                elif level == 'facs':
                    toks = [y.facsimile for y in x.tokens]
                tokens.extend(toks)
        else:
            if level == 'normalized':
                tokens = [x.normalized for x in iii.tokens]
            elif level == 'lemma':
                tokens = [x.lemma for x in iii.tokens]
            elif level == 'facs':
                tokens = [x.facsimile for x in iii.tokens]
        toks = []
        for i in tokens:
            if not i == '-':
                toks.append(i)
        if len(toks) >= 10:
            res[iii.name] = toks
        else:
            print(f'{iii.name} in {iii.ms} was eliminated. Reason: Not enough {level} tokens')
    for z in res.keys():
        doc = " ".join([x for x in res[z] ])
        res[z] = doc
    return res


def get_on_stopwords():
    with open(ON_STOPS, 'r') as f:
        stops = f.read()
    stops = stops.split(',')
    stops = [x.strip() for x in stops]
    return stops


# NLP helper functions
# -------------


def get_vector(corpus: dict, stops: list):
    vectorizer = TfidfVectorizer(stop_words=stops, ngram_range=(1,3))
    w2varr = vectorizer.fit_transform(corpus.values()).toarray()
    return w2varr, corpus.keys()


def euclid_dist(w2varr, labels: list) -> pd.DataFrame:
    euclidean_distances = pd.DataFrame(pairwise_distances(w2varr, metric='euclidean'), index=labels, columns=labels)
    return euclidean_distances


def cos_dist(w2varr, labels: list) -> pd.DataFrame:
        cosine_distances = pd.DataFrame(pairwise_distances(w2varr, metric='cosine'), index=labels, columns=labels) 
        return cosine_distances


def combinator(corpus: dict[str, str], pamph_only: bool = False) -> list[str]:
    if pamph_only:
        all_keys = corpus.keys()
        pamph_mss = ["B1", "P3", "P5", "W1", "To"]
        pamph_keys = [x for x in all_keys if any(ms in x for ms in pamph_mss)]
        res = list(itertools.product(pamph_keys, all_keys))
    else:
        res = itertools.combinations(corpus.keys(), 2)
    return res


def leven_cit_verse(corpus: dict):
    corpus_combinations = combinator(corpus)
    res0 = []
    itcnt = 0
    svcnt = 0
    for i in leven_worker(corpus_combinations, corpus):
        x, y, lev = i
        res0.append((x, y, lev, corpus[x], corpus[y]))
        if itcnt == 10000:
            db = sqlite3.connect("lev-mem.db")
            curse = db.cursor()
            curse.execute("CREATE TABLE IF NOT EXISTS scores (locID integer PRIMARY KEY DEFAULT 0 NOT NULL, v1, v2, score, v1_text, v2_text)")
            curse.executemany("INSERT OR IGNORE INTO scores(v1, v2, score, v1_text, v2_text) VALUES (?, ?, ?, ?, ?)", res0)
            db.commit()
            db.close()
            res0 = []
            itcnt = 0
            svcnt += 1
            print(f"Done a total of {10000*svcnt} pairings.")
        else:
            itcnt += 1        


def leven_worker(combinations, corpus: dict):
    for x,y in combinations:
        leven = fuzz.ratio(corpus[x], corpus[y])
        if leven > 50:
            yield x, y, leven

    
# Analysis functions
# ------------------


def analysis_cycle(corpus: dict, stops: list, fName: str):
    print(f"Now processing {fName}")
    vecCorps, diagLabels = get_vector(corpus, stops)
    eucD = euclid_dist(vecCorps, diagLabels)
    eucD.to_csv(f"{fName}-euclid.csv")  
    print("Euclidian")
    print(eucD)
    cosD = cos_dist(vecCorps, diagLabels)
    print("Cosine")
    print(cosD)    
    cosD.to_csv(f"{fName}-cosine.csv")


def analysis_coordinator():
    latStops = get_latin_stopwords()
    on_stops = get_on_stopwords()
    corpus = corpus_collector_latin()
    analysis_cycle(corpus, latStops, "latin-basic")
    corpus = corpus_collector_latin()
    analysis_cycle(corpus, latStops, "latinLemmatized")
    corpus = corpus_collector_norse('normalized')
    analysis_cycle(corpus, on_stops, "norse-basic")
    corpus = corpus_collector_norse(level='lemma')
    analysis_cycle(corpus, on_stops, "norse-lemmatized")
    corpus = corpus_collector_norse('facs')
    analysis_cycle(corpus=corpus, stops=on_stops, fName='norse-facs')


def versified_lat_leven():
    latStops = get_latin_stopwords()
    corpus = corpus_collector_latin(versify=True)
    leven_cit_verse(corpus)


def run():
    analysis_coordinator()    


if __name__ == '__main__':
    # run()
    versified_lat_leven()