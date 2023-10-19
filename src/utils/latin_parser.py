from bs4 import BeautifulSoup
import pandas as pd
from functools import reduce as red
from utils.util import read_tei
from utils.constants import *


# Region: Class based for i dunno what
# ------

class Token:

    word: str
    lemma: str
    msa: str
    tok_id: int

    def __init__(self, word: str, tok_id: int, lemma: str = "", msa: str = "") -> None:
        self.word = word
        self.lemma = lemma
        self.msa = msa
        self.tok_id = tok_id


class Verse:

    normalized_number: str
    number_on_page: int
    tokens: list[Token]

    def __init__(self, vers_number: str, consecutive_number: int) -> None:
        self.normalized_number = vers_number
        self.number_on_page = consecutive_number
        self.tokens = []
        
    def add_token(self, new_token: Token):
        self.tokens.append(new_token)


class LatinDocument:

    name: str
    shelfmark: str
    verses: list[Verse]
    verses_on_page: dict[int, Verse]
    ordered_verses: dict[str, Verse]

    def __init__(self, abbreviation: str, shelfmark: str) -> None:
        self.name = abbreviation
        self.shelfmark = shelfmark
        self.verses = []
        self.verses_on_page = {} # This is the order of the verses as they appear in the manuscript
        self.ordered_verses = {} # This is the normalized verse number/order, i.e. the numbers Becker uses in his edition
    
    def add_verse(self, new_verse: Verse, verse_number: int, normative_verse_number: str):
        self.verses.append(new_verse)
        self.verses_on_page[verse_number] = new_verse

    
    def to_df(self, as_verse: bool = False):
        data = [(i, word) for i, verse in enumerate(self.verses) for word in verse]
        df = pd.DataFrame(data, columns=["Verse", "Word"])
        if as_verse:
            df = df.groupby(['Verse'])['Word'].apply(" ".join).reset_index()
            df = df.select_dtypes(['object'])
        return df


# Parsing
# -------


def parse_pamphilus(infile: str) -> dict[str, LatinDocument]:
    """This function will process the latin XML of Pamphilus and produce individual objects of the latin document class for every manuscript.
    It will return a dict of LatinDocument"""
    soup = read_tei(infile)
    verses = soup.find_all('v')
    B1 = LatinDocument(abbreviation="B1", shelfmark="Ms. Hamilton 390")
    P3 = LatinDocument(abbreviation="P3", shelfmark="cod. lat. 8430 4°")
    W1 = LatinDocument(abbreviation="W1", shelfmark="cod. 303 HAN MAG 8°")
    To = LatinDocument(abbreviation="To", shelfmark="cod. 102-11")
    P5 = LatinDocument(abbreviation="P5", shelfmark="cod. franc. 25405")
    F = LatinDocument(abbreviation="F", shelfmark="Frankfurt fragm. lat. II 11")
    witness_dict: dict[str, LatinDocument] = {"B1": B1, "P3": P3, "W1": W1, "To": To, "P5": P5, "F": F}
    for verse in verses:       
        current_verse = verse.get('n')
        docs_dict: dict[str, Verse] = {}
        for key in witness_dict.keys():
            docs_dict[key] = Verse(vers_number=current_verse)
        tokens = verse.findAll()
        token_count = 1
        for token in tokens:
            if token.name == 'w':
                variants = token.findAll('var')                
                for variant_reading in variants:
                    local_variants = str(variant_reading.get('variants')).replace(" ", "").split(",")
                    word_on_page = variant_reading.get_text()
                    if "'" in word_on_page:
                        word_on_page = word_on_page.replace("'", "")
                    if '"' in word_on_page:
                        word_on_page = word_on_page.replace('"', '')
                    if "a" in local_variants:
                        for key, val in docs_dict.items():
                            val.add_token(word_on_page, token_count)
                    else:
                        for i in local_variants:
                            docs_dict[i].add_token(word_on_page, token_count)
                token_count += 1
        for key, val in docs_dict.items():
            witness_dict[key].add_verse(val)
    return witness_dict


def parse_amores(soup: BeautifulSoup, versify: bool = False) -> dict[str, str]:
    """The XML of the Amores, retrieved from the Perseus Latin Library, needs special treatment"""
    subsoup = soup.find('group')
    indi_txts = subsoup.find_all('text')
    res = {}
    for txt in indi_txts:
        doc_title = txt.find('head').get_text()
        lines = txt.find_all('l')
        doc_txt = []
        if versify:
            vcount = 1
        for l in lines:
            tokens = l.get_text()
            tokens = tokens.split(" ")
            if versify:
                vdoc_title = f"{doc_title}-{vcount}"
                res[vdoc_title] = " ".join([x for x in tokens])
                vcount += 1
            else:
                for token in tokens:
                    doc_txt.append(token)
        if not versify:
            doc_txt2 = " ".join([x for x in doc_txt])
            res[doc_title] = doc_txt2
    return res


def parse_perseus(infile, versify: bool = False) -> dict:
    # TODO: Prettify
    soup = read_tei(infile)
    fname = infile.rsplit("/", 1)[1]
    res = {}
    if fname == 'ovid.am_lat.xml':
        return parse_amores(soup, versify)
    else:
        doc_title = soup.find('title').get_text()
        if soup.find('note'):
            notes = soup.find_all('note')
            for i in notes:
                i.decompose()
        else:
            print("No notes, yay!")
        if versify:
            try:
                verses = soup.find_all('l')
                hasVerse = True
            except:
                hasVerse = False
            if not hasVerse:
                txt = soup.find('body').get_text()
                verses = txt.splitlines()
            vcount = 1
            for v in verses:
                vtxt = v.get_text()
                vdoc_title = f"{doc_title}-{vcount}"
                res[vdoc_title] = vtxt
                vcount +=1
        else:            
            txt = soup.find('body').get_text()
            res[doc_title] = txt
        return res


def apply_sort() -> dict[dict[str, str]]:
    # TODO: Move up and implement in parse_pamphilus() and LatDoc class
    """This function reads the VERSEORDER excel file and returns a nested dict with the sorting 
    information. The dict is constructed as 
    {Manuscript: {Verse in manuscript order: Verse in normalized i.e. Beckers order}} """
    order_pd = pd.read_excel(VERSEORDER, index_col=None)
    nested_dict = {}
    columns = order_pd.columns
    for index, row in order_pd.iterrows():
        for column in columns:
            if column != 'Base' and row[column] != 'nan':
                if column not in nested_dict:
                    nested_dict[column] = {}
                nested_dict[column][row[column]] = row['Base']
    return nested_dict


def latin_neofyier(infile: str = PAMPHILUS_LATINUS):
    # TODO: Doc
    nodes: list[tuple[str, str, str]] = []
    edge_tuples: list[tuple[str, str, str, str, str]] = []
    lat_docs_dict = parse_pamphilus(infile)
    for key, doc in lat_docs_dict.items():
        for verse in doc.verses:
            current_verse_tuple = (f"'v{key}-{verse.number_on_page}'", VERSETYPE,
                                   f"VerseDipl: '{verse.number_on_page}', VerseNorm: '{verse.normalized_number}', inMS: '{doc.shelfmark}'")
            nodes.append(current_verse_tuple)
            verse_to_ms_edge = (f"'v{key}-{verse.number_on_page}'", f"'{key}'", VERSEINMS, "-", f"{key} - {verse.number_on_page} to MS {key}")
            edge_tuples.append(verse_to_ms_edge)
            for token in verse.tokens:
                current_token_tuple = (f"'{key}-{token.tok_id}'", 'E33_Linguistic_Object',
                                       f"Normalized: '{token.word}', VerseDipl: '{verse.number_on_page}', VerseNorm: '{verse.normalized_number}', inMS: '{key}', pos: '{key}-{token.tok_id}'")
                nodes.append(current_token_tuple)
                token_to_ms_edge = (f"'{key}-{token.tok_id}'", f"'{key}'", 'P56_Is_Found_On', "-", f"{key} - {token.word} to MS {key}")
                edge_tuples.append(token_to_ms_edge)
                token_to_verse_edge = (f"'{key}-{token.tok_id}'", f"'v{key}-{verse.number_on_page}'", 'ZZ2_inVerse', "-", f"{key} - {token.word} to Verse {key}-{verse.number_on_page}")
                edge_tuples.append(token_to_verse_edge)
        all_tokens = [x for y in doc.verses for x in y.tokens]
        all_token_tuples = [(all_tokens[i], all_tokens[i + 1]) for i in range(len(all_tokens) - 1)]
        for i in all_token_tuples:
            current_tuple = (f"'{key}-{i[0].id}'", f"'{key}-{i[1].id}'", 'next', "-", f"{key} - {i[0].word} to next")
            edge_tuples.append(current_tuple)
        verses_diplomatic = list(doc.verses_on_page.keys())
        diplomatic_tuples = [(verses_diplomatic[i], verses_diplomatic[i + 1]) for i in range(len(verses_diplomatic) - 1)]
        for i in diplomatic_tuples:
            current_tuple = (f"'v{key}-{i[0]}'", f"'v{key}-{i[1]}'", 'vNext_dipl', "-", f"{key} - Verse {i[0]} to next Verse {i[1]}")
            edge_tuples.append(current_tuple)
        verses_normalized = list(doc.ordered_verses.keys())
        normalized_tuples = [(verses_normalized[i], verses_normalized[i + 1]) for i in range(len(verses_normalized) - 1)]
        for i in normalized_tuples:
            current_tuple = (f"'v{key}-{i[0]}'", f"'v{key}-{i[1]}'", 'vNext_norm', "-", f"{key} - Verse {i[0]} to next Verse {i[1]}")
            edge_tuples.append(current_tuple)
    
    node_df = pd.DataFrame(nodes, columns=['NodeID', 'NodeLabels', 'NodeProps'])
    edge_df = pd.DataFrame(edge_tuples, columns=['FromNode', 'ToNode', 'EdgeLabels', 'EdgeProps', 'HRF'])
    return node_df, edge_df