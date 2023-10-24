import imp
from typing import Tuple
from bs4 import BeautifulSoup
import bs4
from lxml import etree
from pathlib import Path
import pandas as pd


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
    

    def pretty_print():
        
        pass


class NorseDoc:

    name: str
    ms: str
    sents: list[sent]

    def __init__(self, name: str, manuscript: str) -> None:
        self.name = name
        self.ms = manuscript
        self.sents = []
        self.tokens = []

    
    def add_sent(self, newsent: sent):
        self.sents.append(newsent)
    

    def add_token(self, newtoken: token):
        self.tokens.append(newtoken)
    



class ParallelizedNorseDoc:
    verses: list[vpara]

    def __init__(self, name: str, manuscript: str) -> None:
        self.name = name
        self.ms = manuscript
        self.verses = []

    def add_verse(self, newverse:vpara):
        self.verses.append(newverse)
        

def read_tei(tei_file):
    with open(tei_file, 'r', encoding="UTF-8") as tei:
        soup = BeautifulSoup(tei, from_encoding='UTF-8', features='lxml-xml')
        return soup


def getInfo (soup: BeautifulSoup, get_all: bool = False) -> Tuple[str, str, str]:
    if get_all:
        try:
            msInfo = soup.find("sourcedesc")
        except:
            print("Something is up!")
        if not msInfo:
            try:
                msInfo = soup.find("sourceDesc")
            except:
                print("Ain't nothing here!")
                return
        shelfmark_ = msInfo.find('idno')
        txtName_ = msInfo.find("msName")
        origPlace_ = msInfo.find('origPlace')
        if shelfmark_:
            shelfmark = shelfmark_.get_text()
        else:
            shelfmark = "N/A"
        if txtName_:
            txtName = txtName_.get_text()
        else:
            txtName = "N/A"
        if origPlace_:
            origPlace = origPlace_.get_text()
        else:
            origPlace = "N/A"   
        return shelfmark, txtName, origPlace
    try:
        msInfo = soup.find("sourcedesc")
        shelfmark_ = msInfo.find('idno')
        shelfmark = shelfmark_.get_text()
        txtName_ = msInfo.find("msname")
        txtName = txtName_.get_text()
    except:
        msInfo = soup.find("sourceDesc")
        shelfmark_ = msInfo.find('idno')
        shelfmark = shelfmark_.get_text()
        txtName_ = msInfo.find("msName")
        txtName = txtName_.get_text()

    return shelfmark, txtName


def reg_menota_parse(current_manuscript: NorseDoc, soup: bs4.BeautifulSoup) -> NorseDoc:
    text_proper = soup.find_all('w')

    for word in text_proper:
        lemming = word.get('lemma')
        if lemming is not None:
            lemming = word.get('lemma')
        else:
            lemming = "-"
        facsRaw = word.find('me:facs')
        if facsRaw is not None:
            facsClean = facsRaw.get_text()
        else:
            facsClean = "-"
        diplRaw = word.find('me:dipl')
        if diplRaw is not None:
            diplClean = diplRaw.get_text()
        else:
            diplClean = "-"
        normRaw = word.find('me:norm')
        if normRaw is not None:
            normClean = normRaw.get_text()
        else:
            normClean = "-"
        MSARaw = word.get('me:msa')
        if MSARaw is not None:
            msaClean = word.get('me:msa')
        else:
            msaClean = "-"
        currword = token(normalized=normClean, diplomatic=diplClean, facsimile=facsClean, lemma=lemming, msa=msaClean)
        current_manuscript.add_token(currword)
    return current_manuscript


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
            facsRaw = word.find('me:facs')
            if facsRaw is not None:
                facsClean = facsRaw.get_text()
            else:
                facsClean = "-"
            diplRaw = word.find('me:dipl')
            if diplRaw is not None:
                diplClean = diplRaw.get_text()
            else:
                diplClean = "-"
            normRaw = word.find('me:norm')
            if normRaw is not None:
                normClean = normRaw.get_text()
            else:
                normClean = "-"
            MSARaw = word.get('me:msa')
            if MSARaw is not None:
                msaClean = word.get('me:msa')
            else:
                msaClean = "-"
            currword = token(normalized=normClean, diplomatic=diplClean, facsimile=facsClean, lemma=lemming, msa=msaClean)
            current_verseerse.add_token(currword)
        current_manuscript.add_verse(current_verseerse)
    return current_manuscript


def get_parallelized_text(input_file: str) -> ParallelizedNorseDoc:
    print("Parsing parallelized text...")
    soup = read_tei(input_file)
    ms_sig, txt_name = getInfo(soup)
    current_manuscript = ParallelizedNorseDoc(name=txt_name, manuscript=ms_sig)
    current_manuscript = para_parse(soup=soup, current_manuscript=current_manuscript)
    return current_manuscript


def get_regular_text(input_file: str) -> NorseDoc:
    print("Parsing regular text...")
    soup = read_tei(input_file)
    ms_sig, txt_name = getInfo(soup)
    current_manuscript = NorseDoc(name=txt_name, manuscript=ms_sig)
    current_manuscript = reg_menota_parse(soup=soup, current_manuscript=current_manuscript)
    return current_manuscript




# End Region
# ----------

# Region: For neo4j
# -----------------


def para_neofiyer(input_file: str) -> tuple[list[tuple[str, str, str]], list[tuple[str, str, str, str, str]]]:
    """
        Prepares parallelized Pamphilus-XML for neo4j import. Transforms data into lists of tuples for nodes and edges respecitvely.
        Tuples are packed as follows: (NodeID, NodeLabels, NodeProps) and (FromNode, ToNode, EdgeLabels, EdgeProps, HRF)
    Args:
        input_file (str): Path to XML-file
    Returns:
        tuple[list[tuple[str, str, str]], list[tuple[str, str, str, str, str]]]: Tuple of lists of tuples
    """
    parallel_pamphilus = get_parallelized_text(input_file)
    node_tuple_list: list[tuple[str, str, str]] = []
    edge_tuple_list: list[tuple[str, str, str, str, str]] = []
    current_node_tuple = (f"'{parallel_pamphilus.ms}'", 'E22_Human_Made_Object', f"Signature: 'De La Gardie 4-7', Abbreviation: '{parallel_pamphilus.ms}'")
    node_tuple_list.append(current_node_tuple)
    count1 = 0
    count2 = 1
    key = "DG4-7"
    signature_long = "De La Gardie 4-7"
    current_verse = ""
    not_first = True
    for verse in parallel_pamphilus.verses:
        for word in verse.tokens:
            current_node_tuple = (f"'{key}-{count2}'", 'E33_Linguistic_Object', f"Normalized: '{word.normalized}', Verse: '{verse.vno}', inMS: '{key}', pos: '{key}-{count2}', lemma: '{word.lemma}', paraMS: '{verse.aligned}', paraVerse: '{verse.vno}', VerseNorm: '{verse.vno}'")
            node_tuple_list.append(current_node_tuple)
            if current_verse != verse.vno:
                if not_first:
                    current_edge_tuple = (f"'{key}-{current_verse}'", f"'{key}-{verse.vno}'", 'vNext', '-', f"{key} - Verse {current_verse} to next Verse {verse.vno}")
                    edge_tuple_list.append(current_edge_tuple)
                not_first = False
                current_verse = verse.vno
                current_node_tuple = (f"'v{key}-{current_verse}'", 'ZZ1_Verse', f"vno: '{current_verse}', inMS: '{key}'")
                node_tuple_list.append(current_node_tuple)
                current_edge_tuple = (f"'{key}-{current_verse}'",  signature_long, "ZZ3_VersinMS", '-', f"{key} - Verse {current_verse} to MS {key}")
                edge_tuple_list.append(current_edge_tuple)
                variants = verse.aligned.split(',')
                cleaned_variants = [x.strip() for x in variants]
                # Some parts of the old norse Pamphilus correspond to more than one latin verse, this resolves multiple alignments
                for i in cleaned_variants:
                    if ',' in current_verse:
                        distinct_verses = current_verse.split(',')
                        cleaned_distinct = [x.strip() for x in distinct_verses]
                        for ii in cleaned_distinct:
                            current_edge_tuple = (f"'v{key}-{current_verse}'", f"'v{i}-{ii}'", 'ZZ4_VersPara', '-', f"{key} - Verse {current_verse} to MS {i}")
                            edge_tuple_list.append(current_edge_tuple)
                    elif i == 'x':
                        print(f'No parallel to V{current_verse}')
                    else:
                        current_edge_tuple = (f"'v{key}-{current_verse}'", f"'v{i}-{current_verse}'", 'ZZ4_VersPara', '-', f"{key} - Verse {current_verse} to MS {i}")
                        edge_tuple_list.append(current_edge_tuple)
            current_edge_tuple = (f"'{key}-{count2}'", signature_long, 'P56_Is_Found_On', '-', f"{key} - {word.normalized} to MS {key}")
            edge_tuple_list.append(current_edge_tuple)
            current_edge_tuple = (f"'{key}-{count2}'", f"'v{key}-{current_verse}'", 'ZZ2_inVerse', '-', f"{key} - {word.normalized} to Verse {key}")
            edge_tuple_list.append(current_edge_tuple)
            if count1 > 0:
                current_edge_tuple = (f"'{key}-{count1}'", f"'{key}-{count2}'", 'next', '-', f"{key} - {word.normalized} to next")
                edge_tuple_list.append(current_edge_tuple)
            count1 +=1
            count2 +=1
    return node_tuple_list, edge_tuple_list


def regmenotaParse(input_file) -> pd.DataFrame:
    nodeDF = pd.DataFrame(columns=['NodeID', 'NodeLabels', 'NodeProps'])
    edgeDF = pd.DataFrame(columns=['FromNode', 'ToNode', 'EdgeLabels', 'EdgeProps'])
    soup = read_tei(input_file)
    shelfmark, famName, witID = menota_meta_extractor(soup)
    nodeDF = nodeDF.append({'NodeID': shelfmark, 
                            'NodeLabels': 'E18_Physical_Thing', 
                            'NodeProps': f'Shelfmark: {shelfmark}'}, 
                            ignore_index=True)
    txtWitID = id(shelfmark)
    nodeDF = nodeDF.append({'NodeID': txtWitID,
                            'NodeLabels': 'TX1_Written_Text',
                            'NodeProps': f"Textual_Family: {famName}, witID: {witID}"},
                            ignore_index=True)
    edgeDF = edgeDF.append({'FromNode': txtWitID,
                            'ToNode': shelfmark,
                            'EdgeLabels': 'P56_Is_Found_On'})
    thewords = soup.findAll('w')
    prevNodeID = 0
    for realtalk in thewords:


        lemming = realtalk.get('lemma')
        if lemming is not None:
            lemming = realtalk.get('lemma')
        else:
            lemming = "-"
        try:
            msa = realtalk.get('me:msa')
        except:
            msa = "-"
        diplRaw = realtalk.find('me:dipl')
        if diplRaw is not None:
            diplClean = diplRaw.get_text()
        else:
            diplClean = "-"
        normRaw = realtalk.find('me:norm')
        if normRaw is not None:
            normClean = normRaw.get_text()
        else:
            normClean = "-"
        
        currNodeID = id(realtalk)
        
        if prevNodeID == 0:
            prevNodeID = currNodeID
            nodeDF = nodeDF.append({'NodeID': currNodeID, 
                                    'NodeLabels': 'E33_Linguistic_Object', 
                                    'NodeProps': f"Normalized: {normClean}, Diplomatic: {diplClean}, Lemma: {lemming}, MSA MENOTA: {msa}"}, 
                                    ignore_index=True)
        else:
            nodeDF = nodeDF.append({'NodeID': currNodeID, 
                                    'NodeLabels': 'E33_Linguistic_Object', 
                                    'NodeProps': f"Normalized: {normClean}, Diplomatic: {diplClean}, Lemma: {lemming}, MSA MENOTA: {msa}"}, 
                                    ignore_index=True)
            edgeDF = edgeDF.append({'FromNode': prevNodeID,
                                    'ToNode': currNodeID,
                                    'EdgeLabels': "next"},
                                    ignore_index=True)
            edgeDF = edgeDF.append({'FromNode': currNodeID,
                                    'ToNode': txtWitID,
                                    'EdgeLabels': "P56_Is_Found_On"},
                                    ignore_index=True)
        prevNodeID = currNodeID
    return nodeDF, edgeDF
        

def menota_meta_extractor(inSoup):
    msInfo = inSoup.find("sourceDesc")
    shelfmark = msInfo.msDesc.idno.get_text()
    famName = msInfo.msDesc.msName.get_text()
    witID = f"{famName}-{shelfmark}".replace(" ", "")
    MSpyID = id(msInfo)
    return shelfmark, famName, witID


if __name__ == "__main__":
    testfile = Path("data/training/AM-519a-4to.xml")
    test_stuff = get_regular_text(testfile)