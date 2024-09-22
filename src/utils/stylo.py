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

"""Stylometric analysis functions for the Latin and Old Norse corpora."""


# Helper functions latin
# ----------------------


def get_latin_stopwords(file_path: str = LATIN_STOP_WORDS) -> list[str]:
    """
    Retrieves a list of Latin stopwords from a file.

    Args:
        file_path (str): The path to the file containing the Latin stopwords.

    Returns:
        list[str]: A list of Latin stopwords.

    """
    with open(file_path, 'r') as f:
        stops = f.read()
    stops = stops.split(',')
    stops = [x.strip() for x in stops]
    return stops


def get_pamph(file, versify: bool = False) -> dict[str, str]:
    """
    Retrieves the latin Pamphilus manuscript texts and returns it as a dictionary.

    Args:
        file (str): The path to the Pamphilus file.
        versify (bool, optional): If True, the content will be organized by verse. 
            If False, the content will be organized by page. Defaults to False.

    Returns:
        dict[str, str]: A dictionary containing the content of the Pamphilus file.
            The keys are verse/page identifiers and the values are the corresponding text.

    """
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
    """
    Collects and processes Latin editions from local directory.

    Args:
        lemmatize (bool): Flag indicating whether to lemmatize the corpus. Default is False.
        versify (bool): Flag indicating whether to versify the corpus. Default is False.

    Returns:
        dict: A dictionary containing the processed corpus.

    """
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
    """
    Clean the corpus by removing punctuation and optionally lemmatizing the tokens.

    Args:
        corps (dict[str, str]): A dictionary containing the texts to be cleaned.
        lemmatize (bool, optional): Whether to lemmatize the tokens. Defaults to False.

    Returns:
        dict[str, str]: A dictionary containing the cleaned texts.
    """
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
    """
    Gets the latin cull words; uses the stopwords function to load a different file containing the cull words.

    Args:
        corpus (dict[str, str]): A dictionary where the keys are document names and the values are the corresponding texts.

    Returns:
        dict[str, str]: A new dictionary with the same keys as the input corpus, but with the Latin stopwords removed from the values.
    """
    cull_words = get_latin_stopwords(LATIN_CULL_WORDS)
    res = {}
    for k, v in corpus.items():
        res[k] = " ".join([x for x in v.split() if x not in cull_words])
    return res


# Helper functions old norse
# ---------------------------


def corpus_collector_norse(doc_level: str, use_stops: bool = False, use_mfws: bool = False, mfw_count: int = 200):
    """
    Collects a corpus of Norse texts based on specified parameters.

    Args:
        doc_level (str): The level of text to collect. Possible values are "lemma", "wordform", or "pos".
        use_stops (bool, optional): Whether to use stop words. Defaults to False.
        use_mfws (bool, optional): Whether to use most frequent words. Defaults to False.
        mfw_count (int, optional): The number of most frequent words to consider. Defaults to 200.

    Returns:
        dict: A dictionary containing the collected texts, where the keys are the names of the texts.

    Raises:
        Exception: If `use_stops` is True and `doc_level` is not "lemma".

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
    """
    Reads the contents of the ON_STOPS file and returns a list of stopwords.

    Returns:
        list: A list of stopwords read from the ON_STOPS file.
    """
    with open(ON_STOPS, 'r') as f:
        stops = f.read()
    stops = stops.split(',')
    stops = [x.strip() for x in stops]
    return stops


# NLP helper functions
# -------------

def get_mfws_old_norse(corpus: list[NorseDoc], edition_level: str) -> Counter:
    """
    Get the word counts for Old Norse texts in a given edition level.

    Args:
        corpus (list[NorseDoc]): A list of NorseDoc objects representing the corpus.
        edition_level (str): The edition level to consider. Can be "lemma", "normalized", or "facsimile".

    Returns:
        Counter: A Counter object containing the word counts for the specified edition level.
    """
    if edition_level == "lemma":
        cull_words = pickle.load(open(ON_CULLS_LEMMA, "rb"))
    elif edition_level == "normalized":
        cull_words = pickle.load(open(ON_CULLS_NORMALIZED, "rb"))
    elif edition_level == "facsimile":
        cull_words = []
    all_toks = [getattr(x, edition_level) for y in corpus for x in y.tokens if getattr(x, edition_level) != "-" and getattr(x, edition_level) not in cull_words]
    word_counts = Counter(all_toks)
    return word_counts


def get_vector(corpus: dict):
    """
    Convert a dictionary of text corpus into a TF-IDF vector representation.

    Args:
        corpus (dict): A dictionary containing the text corpus.

    Returns:
        tuple: A tuple containing the TF-IDF vector representation (w2varr) and the keys of the corpus dictionary.

    """
    vectorizer = TfidfVectorizer(ngram_range=(1,3))
    w2varr = vectorizer.fit_transform(corpus.values()).toarray()
    return w2varr, corpus.keys()
    # vectorizer = CountVectorizer(ngram_range=(1,3))
    # w2varr = vectorizer.fit_transform(corpus.values()).toarray()
    # return w2varr, corpus.keys()


def get_tfidfed_vector(corpus: dict, max_doc_freq: float = 0.95, min_doc_freq: float = 0.1, num_features: int | None = None):
    """
    Calculate the TF-IDF vector representation of a given corpus.

    Args:
        corpus (dict): A dictionary containing the documents in the corpus.
        max_doc_freq (float, optional): The maximum document frequency for a term to be included in the TF-IDF calculation. Defaults to 0.95.
        min_doc_freq (float, optional): The minimum document frequency for a term to be included in the TF-IDF calculation. Defaults to 0.1.
        num_features (int | None, optional): The maximum number of features (terms) to include in the TF-IDF calculation. Defaults to None.

    Returns:
        tuple: A tuple containing the TF-IDF matrix representation of the corpus and the keys of the corpus dictionary.
    """
    vectorizer = TfidfVectorizer(ngram_range=(1,3), max_df=max_doc_freq, min_df=min_doc_freq, max_features = num_features)
    w2varr = vectorizer.fit_transform(corpus.values()).toarray()
    return w2varr, corpus.keys()


def cos_dist(w2varr, labels: list) -> pd.DataFrame:
    """
    Calculate the cosine distances between word vectors.

    Parameters:
    w2varr (array-like): The word vectors.
    labels (list): The labels for the word vectors.

    Returns:
    pd.DataFrame: A DataFrame containing the cosine distances between word vectors.
    """
    cosine_distances = pd.DataFrame(pairwise_distances(w2varr, metric='cosine', n_jobs=2), index=labels, columns=labels) 
    return cosine_distances


def _basic_tfidf_analysis(corpus: dict, file_name: str):
    """
    Perform basic TF-IDF analysis on a given corpus.

    Parameters:
    - corpus (dict): A dictionary representing the corpus, where the keys are document names and the values are the document contents.
    - file_name (str): The name of the file to save the analysis results.

    Returns:
    None
    """
    vectorized_corpus, corpus_keys = get_tfidfed_vector(corpus)
    _cosine_dist_analysis(vectorized_corpus, corpus_keys, file_name, file_suffix="tfidf")


def _cosine_dist_analysis(vectorized_corpus, corpus_keys, file_name: str, file_suffix: str):
    """
    Perform cosine distance analysis on a vectorized corpus.

    Args:
        vectorized_corpus: The vectorized representation of the corpus.
        corpus_keys: The keys of the corpus.
        file_name: The name of the output file.
        file_suffix: The suffix to be appended to the output file name.

    Returns:
        None
    """
    print("Cosine")
    cosine_distance = cos_dist(vectorized_corpus, corpus_keys)
    print(cosine_distance)
    cosine_distance.to_csv(f"{STYLO_FOLDER}{file_name}-{file_suffix}.csv")



# Levenshtein functions
# ---------------------


def combinator(corpus: dict[str, str], pamph_only: bool = False, old_norse: bool = False) -> list[str]:
    """
    Generate combinations of keys from the given corpus dictionary.

    Args:
        corpus (dict[str, str]): The corpus dictionary containing the keys.
        pamph_only (bool, optional): If True, generate combinations only from keys containing specific manuscripts. Defaults to False.
        old_norse (bool, optional): If True, generate combinations only from keys containing "Pamph". Defaults to False.

    Returns:
        list[str]: A list of combinations of keys from the corpus dictionary.
    """
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
    """
    Calculate the Levenshtein distance between pairs of verses in a corpus.

    Args:
        corpus (dict): A dictionary containing verses as keys and their corresponding texts as values.

    Returns:
        None
    """
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
    """
    Calculate the Levenshtein distance between slices of text in the given corpus.

    Args:
        corpus (dict[str, str]): A dictionary containing text slices, where the keys are slice IDs and the values are the corresponding text.

    Returns:
        None
    """
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
    """
    Calculate the Levenshtein distance between pairs of strings in the given combinations.

    Args:
        combinations (list): A list of tuples representing pairs of strings.
        corpus (dict): A dictionary containing the strings to compare.

    Yields:
        tuple: A tuple containing the pair of strings and their Levenshtein distance if it is greater than 50.
    """
    for x, y in combinations:
        if x != y:
            leven = fuzz.ratio(corpus[x], corpus[y])
            if leven > 50:
                yield x, y, leven

    
# Analysis functions
# ------------------


def analysis_cycle(corpus: dict, file_name: str, stopped_or_mfwed: bool = False, latin: bool = False):
    """
    Perform analysis on a given corpus.

    Args:
        corpus (dict): The corpus to be analyzed.
        file_name (str): The name of the file being processed.
        stopped_or_mfwed (bool, optional): Flag indicating whether the corpus has been preprocessed with stop words or most frequent words removed. Defaults to False.
        latin (bool, optional): Flag indicating whether the corpus is in Latin. Defaults to False.
    """
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
    """
    Perform revised Latin analysis on the given corpus.

    Args:
        corpus (dict[str, str]): A dictionary containing the corpus data.
        file_name (str): The name of the file being analyzed.

    Returns:
        None
    """
    mfws_list = [400, 600, 800]
    _basic_tfidf_analysis(corpus, file_name)
    mfw_corpus = _latin_culler(corpus)
    for i in mfws_list:
        vectorizer = TfidfVectorizer(ngram_range=(1,3), max_features = i)
        w2varr = vectorizer.fit_transform(mfw_corpus.values()).toarray()
        _cosine_dist_analysis(w2varr, mfw_corpus.keys(), file_name, file_suffix=f"mfwed-{i}")


def latin_stylo():
        """
        Performs analysis on a Latin corpus using different configurations.

        This function collects a Latin corpus and performs analysis on it using two different configurations:
        - First, it uses the `corpus_collector_latin` function to collect the corpus and then calls the `analysis_cycle` function
            with the `latin_basic` configuration.
        - Next, it collects the corpus again, this time with lemmatization enabled, and calls the `analysis_cycle` function
            with the `latin_lemmatized` configuration.

        Parameters:
        None

        Returns:
        None
        """
        corpus = corpus_collector_latin()
        analysis_cycle(corpus, "latin_basic", latin=True)
        corpus = corpus_collector_latin(lemmatize=True)
        analysis_cycle(corpus, "latin_lemmatized", latin=True)
    

def versified_lat_leven():
    """
    This function performs the Levenshtein distance calculation on a corpus of Latin verses.
    It collects the corpus using the `corpus_collector_latin` function with versification enabled,
    and then applies the `leven_cit_verse` function to calculate the Levenshtein distance for each verse.
    """
    corpus = corpus_collector_latin(versify=True)
    leven_cit_verse(corpus)


def norse_stylo_revised():
    """
    Performs analysis on different corpora using various parameters.

    This function collects different corpora for the Norse language, performs analysis on each corpus,
    and saves the results with different file names based on the analysis parameters.

    Parameters:
    None

    Returns:
    None
    """
    corpus = corpus_collector_norse('normalized')
    analysis_cycle(corpus, "norse-basic-new")
    corpus = corpus_collector_norse('lemma')
    analysis_cycle(corpus, "norse-lemmatized-new")
    corpus = corpus_collector_norse('facsimile')
    analysis_cycle(corpus=corpus, file_name='norse-facs-new')
    mfws_list = [100, 200, 300, 400, 600, 800, 1200]
    for i in mfws_list:
        corpus = corpus_collector_norse(doc_level="normalized", use_mfws=True, mfw_count=i)
        analysis_cycle(corpus, f"mfwed-{i}-on-norms-culled", True)
        corpus = corpus_collector_norse(doc_level="lemma", use_mfws=True, mfw_count=i)
        analysis_cycle(corpus, f"mfwed-{i}-on-lemma-culled", True)
        corpus = corpus_collector_norse(doc_level="facsimile", use_mfws=True, mfw_count=i)
        analysis_cycle(corpus, f"mfwed-{i}-on-facs", True)
    corpus = corpus_collector_norse(doc_level="lemma", use_stops=True)
    analysis_cycle(corpus, "on-lemma-stopped", True)


def levenshtein_norse():
    """
    Performs the Levenshtein distance calculation for the Norse corpus.

    This function collects the Norse corpus using the `corpus_collector_norse` function
    and then applies the `leven_cit_window_norse` function to calculate the Levenshtein
    distance for the corpus.

    Parameters:
        None

    Returns:
        None
    """
    corpus = corpus_collector_norse('normalized')
    leven_cit_window_norse(corpus)


def run():
    """
    Executes the stylo analysis functions.

    This function calls the following functions:
    - latin_stylo()
    - norse_stylo_revised()
    - versified_lat_leven()

    Uncomment the line `levenshtein_norse()` if you want to test the levenshtein function. 
    Note that it currently yields no findings.
    """
    latin_stylo()
    norse_stylo_revised()
    versified_lat_leven()
    # levenshtein_norse() TODO: You can uncomment this line if you want to test the levenshtein function. It yields no findings.


if __name__ == '__main__':
    run()