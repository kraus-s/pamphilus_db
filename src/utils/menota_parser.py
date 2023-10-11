import imp
from typing import Tuple
from bs4 import BeautifulSoup
import bs4
from lxml import etree
import os
from pathlib import Path
import pandas as pd


# Region: Class based, idk why
# ----------------------------


class token:

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

    def __init__(self, order: int) -> None:
        self.order = order
        self.tokens = []

    def add_token(self, newtoken: token):
        self.tokens.append(newtoken)
 
        
class vpara:


    def __init__(self, vno: str, var: str) -> None:
        self.vno = vno
        self.aligned = var
        self.tokens = []
    

    def add_token(self, newtoken: token):
        self.tokens.append(newtoken)
    

    def pretty_print():
        
        pass


class doc:

    def __init__(self, name: str, manuscript: str) -> None:
        self.name = name
        self.ms = manuscript
        self.sents = []
        self.tokens = []

    
    def add_sent(self, newsent: sent):
        self.sents.append(newsent)
    

    def add_token(self, newtoken: token):
        self.tokens.append(newtoken)
    



class paradoc:

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


def reg_menota_parse(currMS: doc, soup: bs4.BeautifulSoup) -> doc:
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
        currMS.add_token(currword)
    return currMS


def para_parse(soup, currMS: paradoc):
    paraVerses = soup.findAll('para')
    for indiVerse in paraVerses:
        currVerseNo = indiVerse.get('vn')
        variantBecker = indiVerse.get('var')
        currVerse = vpara(vno=currVerseNo, var=variantBecker)
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
            currVerse.add_token(currword)
        currMS.add_verse(currVerse)
    return currMS


def getText(soup: bs4.BeautifulSoup, isPara: bool):
    if isPara:
        print("I recognized a parallelized text!")
        ms_sig, txt_name = getInfo(soup)
        currMS = paradoc(name=txt_name, manuscript=ms_sig)
        currMS = para_parse(soup=soup, currMS=currMS)
        return currMS
    else:
        print("I will do sentence tokenization!")
        ms_sig, txt_name = getInfo(soup)
        currMS = doc(name=txt_name, manuscript=ms_sig)
        currMS = reg_menota_parse(soup=soup, currMS=currMS)
        return currMS
    


def parse(inFile: str):
    soup = read_tei(inFile)
    if "para" in inFile:
        isPara = True
    else:
        isPara = False
    res = getText(soup, isPara)
    return res


# End Region
# ----------

# Region: For neo4j
# -----------------

menotaEnt = "./data/menota-ents-1.txt"


def paramenotaParse(inFile) -> pd.DataFrame:
    intro = "<!DOCTYPE TEI ["
    outro = " ]>"
    with open(inFile, 'r', encoding="UTF-8") as trash:
        presoup = trash.read()
    with open(menotaEnt, 'r', encoding='UTF-8') as ents:
        treebeard = ents.read()
    pot = etree.fromstring(intro + treebeard + outro + presoup)
    
    soup = BeautifulSoup(etree.tostring(pot, encoding='UTF-8'), features='lxml')
    paraVerses = soup.findAll('para')
    friendlyName = 'DG4-7-Pamph' # TODO: Remove hardcoding
    properIdentifier = friendlyName

    # Throwing MS and witness to the pandas
    pamphBamboo = pd.DataFrame(columns=['Verse', 'Order', 'Lemma', 'Normalized', 'Facsimile', 'MSA', 'Variant'])
    counter = 1 
    for indiVerse in paraVerses:
        currVerse = indiVerse.get('vn')
        variantBecker = indiVerse.get('var')
        words2beParallelized = indiVerse.findAll('w')
        for realtalk in words2beParallelized:         
            lemming = realtalk.get('lemma')
            if lemming is not None:
                lemming = realtalk.get('lemma')
            else:
                lemming = "-"
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
            facsRaw = realtalk.find('me:facs')
            if facsRaw is not None:
                facsClean = facsRaw.get_text()
            else:
                normClean = "-"
            MSARaw = realtalk.get('me:msa')
            if MSARaw is not None:
                msaClean = realtalk.get('me:msa')
            else:
                msaClean = "-"
            pamphBamboo = pamphBamboo.append({'Verse': currVerse, 'Lemma': lemming, 'Normalized': normClean, 'Facsimile': facsClean, 'MSA': msaClean, 'Order': counter, 'Variant': variantBecker}, ignore_index=True)
        counter += 1
    return pamphBamboo


def para_verse_sorter(inDF: pd.DataFrame) -> pd.DataFrame:
    bamboozled = inDF.groupby(['Verse', 'Order']).agg(" ".join).reset_index()
    return bamboozled


def para_neofiyer(infile: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    paraDF = paramenotaParse(infile)
    nodeDF = pd.DataFrame(columns=['NodeID', 'NodeLabels', 'NodeProps'])
    edgeDF = pd.DataFrame(columns=['FromNode', 'ToNode', 'EdgeLabels', 'EdgeProps', 'HRF'])
    nodeDF = nodeDF.append({'NodeID': "'DG4-7'",
                                'NodeLabels': 'E22_Human_Made_Object',
                                'NodeProps': "Signature: 'De La Gardie 4-7', Abbreviation: 'DG4-7'"},
                                ignore_index=True)
    count1 = 0
    count2 = 1
    key = "DG4-7"
    edgeHelperDict2 = {"DG4-7": "De La Gardie 4-7"}
    currV = ""
    notFirst = False
    for index, row in paraDF.iterrows():
        nodeDF = nodeDF.append({
                        'NodeID': f"'{key}-{count2}'",
                        'NodeLabels': 'E33_Linguistic_Object', 
                        'NodeProps': f"Normalized: '{row['Normalized']}', Verse: '{row['Verse']}', inMS: '{key}', pos: '{key}-{count2}', lemma: '{row['Lemma']}', paraMS: '{row['Variant']}', paraVerse: '{row['Verse']}', VerseNorm: '{row['Verse']}'"},
                        ignore_index=True)
        if not currV == row['Verse']:
            if notFirst:
                edgeDF = edgeDF.append({'FromNode': f"'v{key}-{currV}'",
                                        'ToNode': f"'v{key}-{row['Verse']}'",
                                        'EdgeLabels': 'vNext',
                                        'HRF': f"{key} - Verse {currV} to next Verse {row['Verse']}"},
                                        ignore_index=True)
            notFirst = True
            currV = row['Verse']
            nodeDF = nodeDF.append({
                                    'NodeID': f"'v{key}-{currV}'",
                                    'NodeLabels': 'ZZ1_Verse',
                                    'NodeProps': f"vno: '{currV}', inMS: '{key}'"},
                                    ignore_index=True)
            edgeDF = edgeDF.append({'FromNode': f"'v{key}-{currV}'",
                                    'ToNode': f"'{edgeHelperDict2[key]}'",
                                    'EdgeLabels': 'ZZ3_VersinMS',
                                    'HRF': f"{key} - Verse {currV} to MS {key}"},
                                    ignore_index=True)
            variants = row['Variant'].split(',')
            varClean = [x.strip() for x in variants]
            for i in varClean:
                if ',' in currV:
                    indiVs = currV.split(',')
                    cleanVs = [x.strip() for x in indiVs]
                    for ii in cleanVs:
                        edgeDF = edgeDF.append({'FromNode': f"'v{key}-{currV}'",
                                        'ToNode': f"'v{i}-{ii}'",
                                        'EdgeLabels': 'ZZ4_VersPara',
                                        'HRF': f"{key} - Verse {currV} to MS {i}"},
                                        ignore_index=True)
                elif i == 'x':
                    print(f'No parallel to V{currV}')
                else:
                    edgeDF = edgeDF.append({'FromNode': f"'v{key}-{currV}'",
                                        'ToNode': f"'v{i}-{currV}'",
                                        'EdgeLabels': 'ZZ4_VersPara',
                                        'HRF': f"{key} - Verse {currV} to MS {i}"},
                                        ignore_index=True)  
        edgeDF = edgeDF.append({'FromNode': f"'{key}-{count2}'",
                                    'ToNode': f"'{edgeHelperDict2[key]}'",
                                    'EdgeLabels': 'P56_Is_Found_On',
                                    'HRF': f"{key} - {row['Normalized']} to MS {key}"},
                                    ignore_index=True)
        edgeDF = edgeDF.append({'FromNode': f"'{key}-{count2}'",
                                    'ToNode': f"'v{key}-{currV}'",
                                    'EdgeLabels': 'ZZ2_inVerse',
                                    'HRF': f"{key} - {row['Normalized']} to Verse {key}"},
                                    ignore_index=True)
        if count1 > 0:
            edgeDF = edgeDF.append({'FromNode': f"'{key}-{count1}'",
                                                'ToNode': f"'{key}-{count2}'",
                                                'EdgeLabels': 'next',
                                                'HRF': f"{key} - {row['Normalized']} to next"},
                                                ignore_index=True)
        count1 +=1
        count2 +=1
    return nodeDF, edgeDF


def regmenotaParse(inFile) -> pd.DataFrame:
    nodeDF = pd.DataFrame(columns=['NodeID', 'NodeLabels', 'NodeProps'])
    edgeDF = pd.DataFrame(columns=['FromNode', 'ToNode', 'EdgeLabels', 'EdgeProps'])
    soup = read_tei(inFile)
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
    soupsoupsoup = read_tei(testfile)
    myShit = getText(soupsoupsoup)