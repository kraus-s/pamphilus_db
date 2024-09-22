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

    tokens: list[Token]
    verse_number_norm: str

    def __init__(self, verse_number: str) -> None:
        self.tokens = []
        self.verse_number_norm = verse_number
        
    def add_token(self, new_token: Token):
        self.tokens.append(new_token)


class LatinDocument:

    name: str
    shelfmark: str
    verses: dict[str, Verse]
    verses_order_on_page: dict[str, Verse]

    def __init__(self, abbreviation: str, shelfmark: str) -> None:
        self.name = abbreviation
        self.shelfmark = shelfmark
        self.verses = {}
        self.verse_list = []
        self.verses_order_on_page = {}
        self.verse_tuples = apply_sort()[abbreviation]
    
    def add_verse(self, new_verse: Verse):
        self.verses[new_verse.verse_number_norm] = new_verse
        self.verse_list.append(new_verse)
        

    def to_df(self, as_verse: bool = False):
        data = [(i, word) for i, verse in enumerate(self.verses) for word in verse]
        df = pd.DataFrame(data, columns=["Verse", "Word"])
        if as_verse:
            df = df.groupby(['Verse'])['Word'].apply(" ".join).reset_index()
            df = df.select_dtypes(['object'])
        return df
    
    def ms_order_verses(self) -> dict[str, Verse]:
        res = {}
        for number_becker, number_on_page in self.verse_tuples:
            if number_becker != "nan":
                if len(self.verses[number_becker].tokens) > 0:
                    res[number_on_page] = self.verses[number_becker]
        self.verses_order_on_page = res
        return res

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
    token_count = 1
    for verse in verses:       
        current_verse_becker = verse.get('n')
        docs_dict: dict[str, Verse] = {}
        for key in witness_dict.keys():
            try:
                docs_dict[key] = Verse(verse_number=current_verse_becker)
            except KeyError:
                print(f"Verse {current_verse_becker} not found in {key}") # This should only happen when there are verses that are not found in other manuscripts.
        tokens = verse.findAll()
        for token in tokens:
            if token.name == 'w':
                variants = token.findAll('var')                
                for variant_reading in variants:
                    local_variants = str(variant_reading.get('variants')).replace(" ", "").split(",")
                    word_on_page = get_clean_word(variant_reading)
                    if "a" in local_variants:
                        for key, val in docs_dict.items():
                            new_token = Token(word=word_on_page, tok_id=token_count)
                            val.add_token(new_token)
                    else:
                        for i in local_variants:
                            new_token = Token(word=word_on_page, tok_id=token_count)
                            try:
                                docs_dict[i].add_token(new_token)
                            except KeyError:
                                import pdb; pdb.set_trace()
                token_count += 1
        for key, val in docs_dict.items():
            witness_dict[key].add_verse(val)
    for key, val in witness_dict.items():
        val.ms_order_verses()
    return witness_dict


def apply_sort() -> dict[list[tuple[str, int]]]:
    """This function reads the VERSEORDER excel file and returns a nested dict with the sorting 
    information. The dict is constructed as 
    {Manuscript: {Verse in Beckers order: Verse in the order it appears in the manuscript}} """
    order_pd = pd.read_excel(VERSEORDER, index_col=None)
    nested_dict: dict[str, list[tuple[str, int]]] = {}
    columns = order_pd.columns
    for index, row in order_pd.iterrows():
        for column in columns:
            if column != 'Base' and row[column] != 'nan':
                if column not in nested_dict:
                    nested_dict[column] = []
                v_becker = str(row[column]).split(".")[0]
                nested_dict[column].append((v_becker, int(row['Base'])))
    return nested_dict


def get_clean_word(variant_reading: BeautifulSoup):
    word_on_page = variant_reading.get_text()
    if "'" in word_on_page:
        word_on_page = word_on_page.replace("'", "")
    if '"' in word_on_page:
        word_on_page = word_on_page.replace('"', '')
    return word_on_page


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


def parse_perseus(infile: str, versify: bool = False) -> dict[str, str]:
    """This function will process the XML of a Perseus Latin Library document and return a dictionary of the text.
    If versify is True, it will return a dictionary of the text with verse numbers."""
    soup = read_tei(infile)
    res = {}
    if  'ovid.am_lat.xml' in infile:
        return parse_amores(soup, versify)
    else:
        doc_title = soup.find('title').get_text()
        if soup.find('note'):
            notes = soup.find_all('note')
            for i in notes:
                i.decompose()
        if versify:
            verses = soup.find_all('l')
            if not verses:
                txt = soup.find('body').get_text()
                verses = txt.splitlines()
            vcount = 1
            for v in verses:
                vtxt = v.get_text()
                vdoc_title = f"{doc_title}-{vcount}"
                res[vdoc_title] = vtxt
                vcount += 1
        else:
            txt = soup.find('body').get_text()
            res[doc_title] = txt
        return res