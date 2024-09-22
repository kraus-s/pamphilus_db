from bs4 import BeautifulSoup
import bs4
from lxml import etree
from pathlib import Path
import pandas as pd
import requests
import re
import os
import pickle
from utils.constants import *


# Region: Class based, idk why
# ----------------------------


class token:

    normalized: str
    diplomatic: str
    facsimile: str
    lemma: str
    msa: str

    def __init__(self,
                normalized: str,
                diplomatic: str,
                facsimile: str,
                lemma: str,
                msa: str) -> None:
        self.normalized = normalized
        self.diplomatic = diplomatic
        self.facsimile = facsimile
        self.lemma = lemma
        self.msa = msa


class sent:

    order: int
    tokens: list[token]

    def __init__(self, order: int) -> None:
        self.order = order
        self.tokens = []

    def add_token(self, newtoken: token):
        self.tokens.append(newtoken)
 
        
class vpara:

    vno: str
    aligned: str
    tokens: list[token]

    def __init__(self, vno: str, var: str) -> None:
        self.vno = vno
        self.aligned = var
        self.tokens = []
    

    def add_token(self, newtoken: token):
        self.tokens.append(newtoken)


class NorseDoc:

    name: str
    ms: str
    sents: list[sent]
    tokens: list[token]
    lemmatized: bool
    diplomatic: bool
    normalized: bool
    facsimile: bool
    msa: bool
    
    def __init__(self, name: str, manuscript: str, lemmatized: bool, diplomatic: bool, normalized: bool, facsimile: bool, msa: bool) -> None:
        self.name = name
        self.ms = manuscript
        self.sents = []
        self.tokens = []
        self.lemmatized = lemmatized
        self.diplomatic = diplomatic
        self.normalized = normalized
        self.facsimile = facsimile
        self.msa = msa

    
    def add_sent(self, newsent: sent):
        self.sents.append(newsent)
    

    def add_token(self, newtoken: token):
        self.tokens.append(newtoken)
    
    def get_full_text(self, level: str) -> str:
        full_text = ""
        for token in self.tokens:
            if level == "diplomatic":
                full_text += token.diplomatic + " "
            elif level == "normalized":
                full_text += token.normalized + " "
            elif level == "facsimile":
                full_text += token.facsimile + " "
            elif level == "lemma":
                full_text += token.lemma + " "
        return full_text
    



class ParallelizedNorseDoc:
    verses: list[vpara]

    def __init__(self, name: str, manuscript: str) -> None:
        self.name = name
        self.ms = manuscript
        self.verses = []

    def add_verse(self, newverse:vpara):
        self.verses.append(newverse)


def download_and_parse_entities(url) -> dict[str, str]:
    response = requests.get(url)
    response.raise_for_status()  # Ensure the request was successful
    lines = response.text.splitlines()

    entity_mappings = {}
    entity_declaration_pattern = re.compile(r'<!ENTITY\s+(\w+)\s+"&#x([0-9A-Fa-f]+);">')
    for line in lines:
        match = entity_declaration_pattern.search(line)
        if match:
            entity_name, unicode_hex = match.groups()
            entity = f'&{entity_name};'
            unicode_char = chr(int(unicode_hex, 16))
            entity_mappings[entity] = unicode_char
    return entity_mappings


def replace_entities(text: str, entity_mappings: dict[str, str]) -> str:
    for entity, unicode_char in entity_mappings.items():
        text = text.replace(entity, unicode_char)
    return text


def read_tei(tei_file: str, entity_mappings: dict[str, str]) -> BeautifulSoup:
    with open(tei_file, 'r', encoding='utf-8') as file:
        xml_content = file.read()
        xml_content_cleaned = replace_entities(xml_content, entity_mappings)
        soup = BeautifulSoup(xml_content_cleaned, from_encoding='UTF-8', features='lxml-xml')
    return soup


def get_menota_info (soup: BeautifulSoup, get_all: bool = False) -> list[NorseDoc]:
    if get_all:
        try:
            ms_info_ = soup.find("sourcedesc")
        except:
            print("Something is up!")
        if not ms_info_:
            try:
                ms_info_ = soup.find("sourceDesc")
            except Exception as e:
                raise Exception("No Manuscript Info found!") from e
        shelfmark_ = ms_info_.find('idno')
        text_name_ = ms_info_.find("msName")
        place_of_origin_ = ms_info_.find('origPlace')
        if shelfmark_:
            shelfmark = shelfmark_.get_text()
        else:
            shelfmark = "N/A"
        if text_name_:
            txtName = text_name_.get_text()
        else:
            txtName = "N/A"
        if place_of_origin_:
            origPlace = place_of_origin_.get_text()
        else:
            origPlace = "N/A"   
        return shelfmark, txtName, origPlace
    try:
        ms_info_ = soup.find("sourcedesc")
        shelfmark_ = ms_info_.find('idno')
        shelfmark = shelfmark_.get_text()
        text_name_ = ms_info_.find("msname")
        txtName = text_name_.get_text()
    except:
        ms_info_ = soup.find("sourceDesc")
        shelfmark_ = ms_info_.find('idno')
        shelfmark = shelfmark_.get_text()
        text_name_ = ms_info_.find("msName")
        txtName = text_name_.get_text()
    try:
        levels = soup.find("normalization")
        levels = levels["me:level"]
        levels = levels.split()
    except:
        import pdb; pdb.set_trace()
    if "dipl" in levels:
        diplomatic = True
    else:
        diplomatic = False
    if "norm" in levels:
        normalized = True
    else:
        normalized = False
    if "facs" in levels:
        facsimile = True
    else:
        facsimile = False
    interpretations = soup.find("interpretation")
    if interpretations is not None:
        if interpretations["me:lemmatized"] == "completely":
            lemmatized = True
        else:
            lemmatized = False
        if interpretations["me:morphAnalyzed"] == "completely":
            msa = True
        else:   
            msa = False
    else:
        emroon = False
        for string in soup.strings:
            if "emroon" in string:
                emroon = True
        if emroon:
            msa = True
            lemmatized = True
        else:
            msa = False
            lemmatized = False

    parts = _determine_parts(soup)
    current_manuscript = []
    if parts:
        for part in parts:
            current_manuscript.append(NorseDoc(name=part[1], 
                                               manuscript=shelfmark, 
                                               lemmatized=lemmatized, 
                                               diplomatic=diplomatic, 
                                               normalized=normalized, 
                                               facsimile=facsimile, 
                                               msa=msa))
    else:
        current_manuscript.append(NorseDoc(name=txtName, 
                                           manuscript=shelfmark, 
                                           lemmatized=lemmatized, 
                                           diplomatic=diplomatic, 
                                           normalized=normalized, 
                                           facsimile=facsimile, 
                                           msa=msa))

    return current_manuscript


def _determine_parts(soup: bs4.BeautifulSoup) -> list[tuple[str, str]]:
    parts = soup.find("msContents")
    res: list[tuple[str, str]] = []
    if parts is not None:
        parts = parts.find_all("msItem")
        for part in parts:
            part_number = part["n"]
            part_title = part.find("title").get_text()
            res.append((part_number, part_title))
    return res


def reg_menota_parse(current_manuscript: list[NorseDoc], soup: bs4.BeautifulSoup, for_nlp: bool = True) -> list[NorseDoc]:
    res: list[NorseDoc] = []
    if len(current_manuscript) > 1:
        text_body = soup.body.div
        text_body_parts = text_body.find_all("div")
        if len(text_body_parts) == len(current_manuscript):
            for i, part in enumerate(text_body_parts):
                res.append(_token_extraction(part, current_manuscript[i], for_nlp))
                print(f"Finished parsing {res[i].name} from {res[i].ms} with {len(res[i].tokens)} tokens.")
        else:
            for i, ms in enumerate(current_manuscript):
                part = text_body.div
                res.append(_token_extraction(part, ms, for_nlp))
                print(f"Finished parsing {res[i].name} from {res[i].ms} with {len(res[i].tokens)} tokens.")
                part.decompose()
    else:
        res.append(_token_extraction(soup, current_manuscript[0], for_nlp))
        print(f"Finished parsing {res[0].name} from {res[0].ms} with {len(res[0].tokens)} tokens.")
    return res


def _token_extraction(soup: bs4.BeautifulSoup, current_doc: NorseDoc, for_nlp: bool = True) -> NorseDoc:
    text_proper = soup.find_all('w')
    for word in text_proper:
        lemming = word.get('lemma')
        if lemming is not None:
            lemming = word.get('lemma')
            if "-" in lemming:
                lemming = lemming.replace("-", "")
            if "(" in lemming:
                lemming = lemming.replace("(", "")
            if ")" in lemming:
                lemming = lemming.replace(")", "")
            lemming = lemming.lower()
            weird_lemmatization_dict = {"kaupsskip": "kaupskip", "slǫngva": "sløngva", "þrøngva þrøngja": "þrøngva"}
            for k, v in weird_lemmatization_dict.items():
                if lemming == k:
                    lemming = v
            if lemming == "?":
                lemming = "-"
        else:
            lemming = "-"
        facsimile_raw = word.find('me:facs')
        if facsimile_raw is not None:
            facsimile_clean = facsimile_raw.get_text()
        else:
            facsimile_clean = "-"
        diplomatic_raw = word.find('me:dipl')
        if diplomatic_raw is not None:
            diplomatic_clean = diplomatic_raw.get_text()
        else:
            diplomatic_clean = "-"
        normalized_raw = word.find('me:norm')
        if normalized_raw is not None:
            normalized_clean = normalized_raw.get_text()
        else:
            normalized_clean = "-"
        msa_raw = word.get('me:msa')
        if msa_raw is not None:
            msa_clean = word.get('me:msa')
        else:
            msa_clean = "-"
        token_parts_list_final: list[str] = []
        for i in [normalized_clean, diplomatic_clean, facsimile_clean, lemming]:
            weird_chars_dict = {"ҩoogon;": "ǫ", "ҩoslashacute;": "ǿ", "ҩaeligacute;": "ǽ", "lͣ": "l", "io": "jo", "ió": "jó", "ia": "ja"}
            for k, v in weird_chars_dict.items():
                if k in i:
                    i = i.replace(k, v)
            if for_nlp:
                i = i.lower()
            token_parts_list_final.append(i.replace(" ", "").replace("-", ""))
        currword = token(normalized=token_parts_list_final[0], diplomatic=token_parts_list_final[1], facsimile=token_parts_list_final[2], lemma=token_parts_list_final[3], msa=msa_clean)
        current_doc.add_token(currword)
    return current_doc


def para_parse(soup, current_manuscript: ParallelizedNorseDoc) -> ParallelizedNorseDoc:
    paraVerses = soup.findAll('para')
    for indiVerse in paraVerses:
        current_verseerseNo = indiVerse.get('vn')
        variantBecker = indiVerse.get('var')
        current_verseerse = vpara(vno=current_verseerseNo, var=variantBecker)
        allWords = indiVerse.find_all('w')
        for word in allWords:
            lemming = word.get('lemma')
            if lemming is not None:
                lemming = word.get('lemma')
            else:
                lemming = "-"
            facsimile_raw = word.find('me:facs')
            if facsimile_raw is not None:
                facsimile_clean = facsimile_raw.get_text()
            else:
                facsimile_clean = "-"
            diplomatic_raw = word.find('me:dipl')
            if diplomatic_raw is not None:
                diplomatic_clean = diplomatic_raw.get_text()
            else:
                diplomatic_clean = "-"
            normalized_raw = word.find('me:norm')
            if normalized_raw is not None:
                normalized_clean = normalized_raw.get_text()
            else:
                normalized_clean = "-"
            msa_raw = word.get('me:msa')
            if msa_raw is not None:
                msa_clean = word.get('me:msa')
            else:
                msa_clean = "-"
            currword = token(normalized=normalized_clean, diplomatic=diplomatic_clean, facsimile=facsimile_clean, lemma=lemming, msa=msa_clean)
            current_verseerse.add_token(currword)
        current_manuscript.add_verse(current_verseerse)
    return current_manuscript


def get_parallelized_text(input_file: str) -> ParallelizedNorseDoc:
    """This is basically only used for Pamphilus saga atm, so its quite specific."""
    print("Parsing parallelized text...")
    if not os.path.exists(PAMPHILUS_SAGA_PICKLE):
        entity_dict = download_and_parse_entities("http://www.menota.org/menota-entities.txt")
        soup = read_tei(input_file, entity_dict)
        current_manuscript = get_menota_info(soup)
        current_manuscript = ParallelizedNorseDoc(name=current_manuscript.name, manuscript=current_manuscript.ms)
        current_manuscript = para_parse(soup=soup, current_manuscript=current_manuscript)
        pickle.dump(current_manuscript, open(PAMPHILUS_SAGA_PICKLE, "wb"))
    else:
        current_manuscript = pickle.load(open(PAMPHILUS_SAGA_PICKLE, "rb"))
    return current_manuscript


def get_regular_text(input_file: str, entity_dict: dict[str, str]) -> list[NorseDoc]:
    print("Parsing regular text...")
    soup = read_tei(input_file, entity_dict)
    current_manuscript = get_menota_info(soup)
    current_manuscript = reg_menota_parse(soup=soup, current_manuscript=current_manuscript)
    print(f"Finished parsing {current_manuscript[0].ms} with {len(current_manuscript)} parts and {sum([len(x.tokens) for x in current_manuscript])} tokens.")
    return current_manuscript
        

def menota_meta_extractor(inSoup):
    ms_info_ = inSoup.find("sourceDesc")
    shelfmark = ms_info_.msDesc.idno.get_text()
    famName = ms_info_.msDesc.msName.get_text()
    witID = f"{famName}-{shelfmark}".replace(" ", "")
    MSpyID = id(ms_info_)
    return shelfmark, famName, witID


if __name__ == "__main__":
    print("This is a utility module, not meant to be run as a script.")