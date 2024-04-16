import itertools
import utils.latin_parser as latin_parser
from utils.constants import *
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import pairwise_distances
import pandas as pd
import glob
import sqlite3
from rapidfuzz import fuzz
import spacy
from utils.menota_parser import NorseDoc
from utils.util import load_data
from collections import Counter
import pickle
from sklearn.feature_extraction.text import CountVectorizer


# Helper functions latin
# ----------------------


def get_latin_stopwords(file_path: str = LATIN_STOP_WORDS) -> list[str]:
    with open(file_path, 'r') as f:
        stops = f.read()
    stops = stops.split(',')
    stops = [x.strip() for x in stops]
    return stops


def get_pamph(file, versify: bool = False) -> dict[str, str]:
    pamph = latin_parser.parse_pamphilus(file)
    res = {}
    if versify:
        for i in pamph:
            for ii in pamph[i].verses:
                vttl = f"{i}-{ii.vno}"
                txt = " ".join([x for x in ii.tokens])
                res[vttl] = txt              
    else:
        for i in pamph:
            doc = []
            for k, ii in pamph[i].verses_order_on_page.items():
                for iii in ii.tokens:
                    doc.append(iii.word)
            doc2 = " ".join([x for x in doc])
            res[i] = doc2
    return res


def corpus_collector_latin(lemmatize: bool = False, versify: bool = False) -> dict:
    file_list = glob.glob(f"{LATIN_CORPUS_FILES}**/*.xml", recursive=True)
    corpus_dict = {}
    for f in file_list:
        print(f"Now opening {f}")
        if 'pamph' in f:
            i = get_pamph(f, versify)
        else:
            i = latin_parser.parse_perseus(f, versify)
        corpus_dict.update(i)
    res = corpus_cleaner(corpus_dict, lemmatize)
    return res


def corpus_cleaner(corps: dict[str, str], lemmatize: bool = False) -> dict[str, str]:
    res = {}
    nlp = spacy.load('la_core_web_lg')
    nlp.max_length = 10000000
    text_tuples = [(corps[x], {"text_id": x}) for x in corps.keys()]
    for doc, context in nlp.pipe(text_tuples, as_tuples=True, batch_size=5):
        if lemmatize:
            res[context["text_id"]] = ' '.join(tok.lemma_ for tok in doc if not tok.is_punct)
        else:
            res[context["text_id"]] = ' '.join(tok.norm_ for tok in doc if not tok.is_punct)
    return res


def _latin_culler(corpus: dict[str, str]) -> dict[str, str]:
    cull_words = get_latin_stopwords(LATIN_CULL_WORDS)
    res = {}
    for k, v in corpus.items():
        res[k] = " ".join([x for x in v.split() if x not in cull_words])
    return res


# Helper functions old norse
# ---------------------------


def corpus_collector_norse(doc_level: str, use_stops: bool = False, use_mfws: bool = False, mfw_count: int = 200):
    """Params: level(str): "normalized" or "lemma" or "facsimile"
    """
    
    if use_stops and doc_level == "lemma":
        stop_words = get_on_stopwords()
    elif use_stops and doc_level != "lemma":
        raise Exception("Not implemented!")
    
    menota_docs = load_data()

    if use_mfws:
        mfws_raw = get_mfws_old_norse(menota_docs, doc_level)
        mfws = [x[0] for x in mfws_raw.most_common(mfw_count) if len(x[0]) > 1]
        print(mfws)
    res = {}
    for i in menota_docs:
        if use_stops:
            doc = " ".join([getattr(x, doc_level) for x in i.tokens if x.lemma not in stop_words])
        elif use_mfws:
            doc = " ".join([getattr(x, doc_level) for x in i.tokens if getattr(x, doc_level) in mfws])
        else:
            doc = " ".join([getattr(x, doc_level) for x in i.tokens])
        doc = doc.replace("-", "")
        if len(doc) - doc.count(" ") > 100:
            txt_ms_name = f"{i.name}-{i.ms}"
            res[txt_ms_name] = doc
    return res


def get_on_stopwords():
    with open(ON_STOPS, 'r') as f:
        stops = f.read()
    stops = stops.split(',')
    stops = [x.strip() for x in stops]
    return stops


# NLP helper functions
# -------------

def get_mfws_old_norse(corpus: list[NorseDoc], edition_level: str) -> Counter:
    if edition_level == "lemma":
        cull_words = pickle.load(open(ON_CULLS_LEMMA, "rb"))
    elif edition_level == "normalized":
        cull_words = pickle.load(open(ON_CULLS_NORMALIZED, "rb"))
    else:
        raise Exception("Not implemented!")
    all_toks = [getattr(x, edition_level) for y in corpus for x in y.tokens if getattr(x, edition_level) != "-" and getattr(x, edition_level) not in cull_words]
    word_counts = Counter(all_toks)
    return word_counts


def get_vector(corpus: dict):
    vectorizer = TfidfVectorizer(ngram_range=(1,3))
    w2varr = vectorizer.fit_transform(corpus.values()).toarray()
    return w2varr, corpus.keys()
    # vectorizer = CountVectorizer(ngram_range=(1,3))
    # w2varr = vectorizer.fit_transform(corpus.values()).toarray()
    # return w2varr, corpus.keys()


def get_tfidfed_vector(corpus: dict, max_doc_freq: float = 0.95, min_doc_freq: float = 0.1, num_features: int | None = None):
    vectorizer = TfidfVectorizer(ngram_range=(1,3), max_df=max_doc_freq, min_df=min_doc_freq, max_features = num_features)
    w2varr = vectorizer.fit_transform(corpus.values()).toarray()
    return w2varr, corpus.keys()


def cos_dist(w2varr, labels: list) -> pd.DataFrame:
    cosine_distances = pd.DataFrame(pairwise_distances(w2varr, metric='cosine', n_jobs=2), index=labels, columns=labels) 
    return cosine_distances


def combinator(corpus: dict[str, str], pamph_only: bool = False, old_norse: bool = False) -> list[str]:
    if pamph_only and not old_norse:
        all_keys = corpus.keys()
        pamph_mss = ["B1", "P3", "P5", "W1", "To"]
        pamph_keys = [x for x in all_keys if any(ms in x for ms in pamph_mss)]
        res = list(itertools.product(pamph_keys, all_keys))
    elif pamph_only and old_norse:
        all_keys = corpus.keys()
        pamph_keys = [x for x in all_keys if "Pamph" in x]
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
            curse.execute("CREATE TABLE IF NOT EXISTS rat_scores (locID integer PRIMARY KEY DEFAULT 0 NOT NULL, v1, v2, score, v1_text, v2_text)")
            curse.executemany("INSERT OR IGNORE INTO rat_scores(v1, v2, score, v1_text, v2_text) VALUES (?, ?, ?, ?, ?)", res0)
            db.commit()
            db.close()
            res0 = []
            itcnt = 0
            svcnt += 1
            print(f"Done a total of {10000*svcnt} pairings.")
        else:
            itcnt += 1        


def leven_cit_window_norse(corpus: dict[str, str]):
    slices_dict = {}
    for i in corpus:
        toks = corpus[i].split()
        for j in range(0, len(toks), 10):
            slices_dict[f"{i}-{j}"] = " ".join(toks[j:j+30])
    slice_combinations = combinator(slices_dict, pamph_only=True, old_norse=True)
    res0 = []
    itcnt = 0
    svcnt = 0

    for i in leven_worker(slice_combinations, slices_dict):
        x, y, lev = i
        res0.append((x, y, lev, slices_dict[x], slices_dict[y]))
        if lev > 80:
            print(f"{x} and {y} have a leven of {lev}")
        if itcnt == 500:
            db = sqlite3.connect(LEVEN_DB_ON)
            curse = db.cursor()
            curse.execute("CREATE TABLE IF NOT EXISTS rat_scores (locID integer PRIMARY KEY DEFAULT 0 NOT NULL, sent1, sent2, score, s1_text, s2_text)")
            curse.executemany("INSERT OR IGNORE INTO rat_scores(sent1, sent2, score, s1_text, s2_text) VALUES (?, ?, ?, ?, ?)", res0)
            db.commit()
            db.close()
            res0 = []
            itcnt = 0
            svcnt += 1
            print(f"Done a total of {500*svcnt} pairings.")
        else:
            itcnt += 1 
        

def leven_worker(combinations, corpus: dict):
    for x, y in combinations:
        if x != y:
            leven = fuzz.ratio(corpus[x], corpus[y])
            if leven > 50:
                yield x, y, leven

    
# Analysis functions
# ------------------


def analysis_cycle(corpus: dict, file_name: str, stopped_or_mfwed: bool = False, latin: bool = False):
    print(f"Now processing {file_name}")
    if latin:
        _revised_latin_analysis(corpus, file_name)
    else:
        vectorized_corpus, corpus_keys = get_vector(corpus)
        cosine_distance = cos_dist(vectorized_corpus, corpus_keys)
        print("Cosine")
        print(cosine_distance)    
        cosine_distance.to_csv(f"{STYLO_FOLDER}{file_name}-cosine.csv")
        if not stopped_or_mfwed:
            _basic_tfidf_analysis(corpus, file_name)


def _revised_latin_analysis(corpus: dict[str, str], file_name: str):
    mfws_list = [400, 600, 800]
    _basic_tfidf_analysis(corpus, file_name)
    mfw_corpus = _latin_culler(corpus)
    for i in mfws_list:
        vectorizer = TfidfVectorizer(ngram_range=(1,3), max_features = i)
        w2varr = vectorizer.fit_transform(mfw_corpus.values()).toarray()
        _cosine_dist_analysis(w2varr, mfw_corpus.keys(), file_name, file_suffix=f"mfwed-{i}")


def _basic_tfidf_analysis(corpus: dict, file_name: str):
    vectorized_corpus, corpus_keys = get_tfidfed_vector(corpus)
    _cosine_dist_analysis(vectorized_corpus, corpus_keys, file_name, file_suffix="tfidf")


def _cosine_dist_analysis(vectorized_corpus, corpus_keys, file_name: str, file_suffix: str):
    print("Cosine")
    cosine_distance = cos_dist(vectorized_corpus, corpus_keys)
    print(cosine_distance)
    cosine_distance.to_csv(f"{STYLO_FOLDER}{file_name}-{file_suffix}.csv")


def latin_stylo():
    corpus = corpus_collector_latin()
    analysis_cycle(corpus, "latin_basic", latin=True)
    corpus = corpus_collector_latin(lemmatize=True)
    analysis_cycle(corpus, "latin_lemmatized", latin=True)
    

def versified_lat_leven():
    corpus = corpus_collector_latin(versify=True)
    leven_cit_verse(corpus)


def run():
    latin_stylo()
    versified_lat_leven()    


def norse_stylo_revised():
    corpus = corpus_collector_norse('normalized')
    analysis_cycle(corpus, "norse-basic-new")
    corpus = corpus_collector_norse('lemma')
    analysis_cycle(corpus, "norse-lemmatized-new")
    corpus = corpus_collector_norse('facsimile')
    analysis_cycle(corpus=corpus, file_name='norse-facs-new')
    mfws_list = [100, 200, 300, 400, 600, 800]
    for i in mfws_list:
        corpus = corpus_collector_norse(doc_level="normalized", use_mfws=True, mfw_count=i)
        analysis_cycle(corpus, f"mfwed-{i}-on-norms-culled", True)
        corpus = corpus_collector_norse(doc_level="lemma", use_mfws=True, mfw_count=i)
        analysis_cycle(corpus, f"mfwed-{i}-on-lemma-culled", True)
    corpus = corpus_collector_norse(doc_level="lemma", use_stops=True)
    analysis_cycle(corpus, "on-lemma-stopped", True)


def levenshtein_norse():
    corpus = corpus_collector_norse('normalized')
    leven_cit_window_norse(corpus)

if __name__ == '__main__':
    run()