from os import read
import neo4j
from bs4 import BeautifulSoup
import lxml
from pathlib import Path
import pandas as pd
from pandas.io.pytables import FrameFixed
import xlrd
from multiprocessing.pool import ThreadPool


# Constants
###########


safeHouse = Path('.')
latMat = Path('latMat/')
norseMat = Path('norseMat/')
bowlMat = Path('bowlMat/')
paraMat = Path('paraMat/')

remoteDB = "philhist-sven-1.philhist.unibas.ch"
localDB = "localhost"


# Util

def read_tei(tei_file) -> BeautifulSoup:
    """Takes a file path as input and returns a beautifulsoup object."""
    with open(tei_file, 'r', encoding="UTF-8") as tei:
        soup = BeautifulSoup(tei, "lxml-xml", from_encoding='UTF-8')
        return soup


# Parser functions


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


def latParser(inFile):
    soup = read_tei(inFile)
    verses = soup.find_all('v')
    nodeDF = pd.DataFrame(columns=['NodeID', 'NodeLabels', 'NodeProps'])
    edgeDF = pd.DataFrame(columns=['FromNode', 'ToNode', 'EdgeLabels', 'EdgeProps'])
    prevNodeID = 0
    msDict = {'B1': 'Ms. Hamilton 390', 'P3': 'cod. lat. 8430 4°', 'To': 'cod. 102-11', 'W1': 'cod. 303 HAN MAG 8°'}
    for key, value in msDict.items():
        nodeDF = nodeDF.append({'NodeID': value,
                                'NodeLabels': 'E18_Physical_Thing',
                                'NodeProps': f"Signature: {value}, Abbreviation: {key}"},
                                ignore_index=True)
    

    for indiVerse in verses:
        
        currVerse = indiVerse.get('n')
        words2beWorked = indiVerse.findAll('w')
        for realtalk in words2beWorked:
            variants = realtalk.findAll('var')
            countVars = len(variants)
            if countVars == 1:
                for ms in msList:
                    variants = realtalk.find('var')
                    actualWord = variants.get_text()
                    currID = id(actualWord)
                    nodeDF = nodeDF.append({'NodeID': currID, 
                                            'NodeLabels': 'E33_Linguistic_Object', 
                                            'NodeProps': f"Normalized: {actualWord}"},
                                            ignore_index=True)
                    
                    if not prevNodeID == 0:
                        edgeDF = edgeDF.append({'FromNode': prevNodeID,
                                                'ToNode': currID,
                                                'EdgeLabels': 'next'},
                                                ignore_index=True)
                        edgeDF = edgeDF.append({'FromNode': currID})


if __name__ == "__main__":
    regmenotaParse("norseMat/DG-4at7-Streng.xml")