from bs4 import BeautifulSoup
import pandas as pd
from functools import reduce as red
from utils.util import read_tei
import spacy
from utils.constants import *


# Region: Class based for i dunno what
# ------

class token:

    def __init__(self, word: str, lemma: str = "", msa: str = "") -> None:
        self.word = word
        self.lemma = lemma
        self.msa = msa


class verse:

    def __init__(self, versNumber: str) -> None:
        self.vno = versNumber
        self.tokens = []
        
    def add_token(self, newToken: token):
        self.tokens.append(newToken)


class latDoc:

    def __init__(self, abbreviation: str, shelfmark: str) -> None:
        self.name = abbreviation
        self.shelfmark = shelfmark
        self.verses = []
        self.ordered_verses = {}
    
    def add_verse(self, new_verse: verse):
        self.verses.append(new_verse)
    
    def order_verses(self, versObj, verseOrder):
        self.ordered_verses[verseOrder] = versObj


# Parsing
# -------


def parse_pamphilus(infile: str) -> dict[str, latDoc]:
    soup = read_tei(infile)
    verses = soup.find_all('v')
    B1 = latDoc(abbreviation="B1", shelfmark="Ms Hamilton 390")
    P3 = latDoc(abbreviation="P3", shelfmark="BNF cod. lat. 8430")
    W1 = latDoc(abbreviation="W1", shelfmark="cod. 303")
    To = latDoc(abbreviation="To", shelfmark="cod. 102")
    P5 = latDoc(abbreviation="P5", shelfmark="BNF cod. franc. 25405")
    res = {"B1": B1, "P3": P3, "To": To, "W1": W1, "P5": P5}
    for indiVerse in verses:
        
        currVerse_ = indiVerse.get('n')
        currVerseB1 = verse(versNumber=currVerse_)
        currVerseP3 = verse(versNumber=currVerse_)
        currVerseW1 = verse(versNumber=currVerse_)
        currVerseTo = verse(versNumber=currVerse_)
        curr_verse_p5 = verse(versNumber=currVerse_)
        currVList = [currVerseB1, currVerseP3, currVerseW1, currVerseTo, curr_verse_p5]
        words2beWorked = indiVerse.findAll()
        for realtalk in words2beWorked:
            if realtalk.name == 'w':
                variants = realtalk.findAll('var')                
                for indiVari in variants:
                    variantesConcretes = indiVari.get('variants')
                    actualWord = indiVari.get_text()
                    if "'" in actualWord:
                        actualWord = actualWord.replace("'", "")
                    if '"' in actualWord:
                        actualWord = actualWord.replace('"', '')
                    if "a" in variantesConcretes:
                        for i in currVList:
                            i.add_token(actualWord)
                    if "B1" in variantesConcretes:
                        currVerseB1.add_token(actualWord)
                    if "P3" in variantesConcretes:
                        currVerseP3.add_token(actualWord)
                    if "To" in variantesConcretes:
                        currVerseTo.add_token(actualWord)
                    if "W1" in variantesConcretes:
                        currVerseW1.add_token(actualWord)
                    if "P5" in variantesConcretes:
                        curr_verse_p5.add_token(actualWord)
        B1.add_verse(currVerseB1)
        P3.add_verse(currVerseP3)
        To.add_verse(currVerseTo)
        W1.add_verse(currVerseW1)
        P5.add_verse(curr_verse_p5)
    return res


def parse_perseus(infile, versify: bool = False) -> dict:
    soup = read_tei(infile)
    fname = infile.rsplit("/", 1)[1]
    res = {}
    if fname == 'ovid.am_lat.xml':
        subsoup = soup.find('group')
        indiTxts = subsoup.find_all('text')
        for txt in indiTxts:
            ttl = txt.find('head').get_text()
            lines = txt.find_all('l')
            docTxt = []
            if versify:
                vcount = 1
            for l in lines:
                tokens = l.get_text()
                tokens = tokens.split(" ")
                if versify:
                    vttl = f"{ttl}-{vcount}"
                    res[vttl] = " ".join([x for x in tokens])
                    vcount += 1
                else:
                    for token in tokens:
                        docTxt.append(token)
            if not versify:
                docTxt2 = " ".join([x for x in docTxt])
                res[ttl] = docTxt2
        return res
    else:
        ttl = soup.find('title').get_text()
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
                vttl = f"{ttl}-{vcount}"
                res[vttl] = vtxt
                vcount +=1
        else:            
            txt = soup.find('body').get_text()
            res[ttl] = txt
        return res

        


# End Region
# ----------


# Region: For neo4j
# -----------------

# Latin functions
#----------------


def read_latin_xml(infile):
    soup = read_tei(infile)
    verses = soup.find_all('v')
    witnessB1Bamboo = pd.DataFrame(columns=['Verse', 'Word'])
    witnessP3Bamboo = pd.DataFrame(columns=['Verse', 'Word'])
    witnessToBamboo = pd.DataFrame(columns=['Verse', 'Word'])
    witnessW1Bamboo = pd.DataFrame(columns=['Verse', 'Word'])
    witnessP5Bamboo = pd.DataFrame(columns=['Verse', 'Word'])
  
    for indiVerse in verses:
        
        currVerse = indiVerse.get('n')
        currVerseReps = []

        words2beWorked = indiVerse.findAll()
        for realtalk in words2beWorked:
            if realtalk.name == 'w':
                variants = realtalk.findAll('var')                
                for indiVari in variants:
                    variantesConcretes = indiVari.get('variants')
                    actualWord = indiVari.get_text()
                    if "'" in actualWord:
                        actualWord = actualWord.replace("'", "")
                    if '"' in actualWord:
                        actualWord = actualWord.replace('"', '')
                    if "a" in variantesConcretes:
                        witnessB1Bamboo = witnessB1Bamboo.append({'Verse': currVerse, 'Word': actualWord}, ignore_index=True)
                        witnessP3Bamboo = witnessP3Bamboo.append({'Verse': currVerse, 'Word': actualWord}, ignore_index=True)
                        witnessToBamboo = witnessToBamboo.append({'Verse': currVerse, 'Word': actualWord}, ignore_index=True)
                        witnessW1Bamboo = witnessW1Bamboo.append({'Verse': currVerse, 'Word': actualWord}, ignore_index=True)
                    if "B1" in variantesConcretes:
                        witnessB1Bamboo = witnessB1Bamboo.append({'Verse': currVerse, 'Word': actualWord}, ignore_index=True)
                        currVerseReps.append('B1')
                    if "P3" in variantesConcretes:
                        witnessP3Bamboo = witnessP3Bamboo.append({'Verse': currVerse, 'Word': actualWord}, ignore_index=True)
                        currVerseReps.append('P3')
                    if "To" in variantesConcretes:
                        witnessToBamboo = witnessToBamboo.append({'Verse': currVerse, 'Word': actualWord}, ignore_index=True)
                        currVerseReps.append('To')
                    if "W1" in variantesConcretes:
                        witnessW1Bamboo = witnessW1Bamboo.append({'Verse': currVerse, 'Word': actualWord}, ignore_index=True)
                        currVerseReps.append('W1')
    return witnessB1Bamboo, witnessP3Bamboo, witnessToBamboo, witnessW1Bamboo


def latin_para_import(infile, outputDF: str = 'merged'):
    '''Reads the parallelized XML and returns DF.
    Args:
    outputDF: if grouped returns all witnesses in one DF, column names reflecting MS abbrs.
    if outputDF == 'dict' returns individual DFs for each witness '''
    Berlin1, Paris3, Toledo, Wien1 = read_latin_xml(infile)
    Berlin1 = Berlin1.groupby(['Verse'])['Word'].apply(" ".join).reset_index()
    Paris3 = Paris3.groupby(['Verse'])['Word'].apply(" ".join).reset_index()
    Toledo = Toledo.groupby(['Verse'])['Word'].apply(" ".join).reset_index()
    Wien1 = Wien1.groupby(['Verse'])['Word'].apply(" ".join).reset_index()
    zoo = {"B1": Berlin1,"P3": Paris3, "To": Toledo, "W1": Wien1}
    for key, value in zoo.items():
        df_obj = value.select_dtypes(['object'])
        df1 = df_obj.apply(lambda x: x.str.strip())
        newDF = apply_sort(df1, key)
        updog = {key: newDF}
        zoo.update(updog)
    if outputDF == 'dict':
        return zoo
    if outputDF == 'merged':
        zooList = []
        for key, value in zoo.items():
            newDF = value.rename(columns={'Word': key})
            zooList.append(newDF)
        df_merged = red(lambda left,right: pd.merge(left,right, on=['Verse'], how='outer'), zooList)
        return df_merged


def apply_sort(inDF: pd.DataFrame, currMS: str) -> pd.DataFrame:
    orderPD = pd.read_excel(VERSEORDER, index_col=None, usecols=['Base', currMS])
    orderPD[currMS] = orderPD[currMS].astype(str).replace('\.0', '', regex=True)
    orderPD['Base'] = orderPD['Base'].astype(int)
    orderPD.dropna(inplace=True)
    newDF = pd.merge(left=inDF, right=orderPD, left_on='Verse', right_on=currMS, how='left')
    newDF.set_index('Base', inplace=True)
    newDF = newDF.sort_index()
    newDF = newDF.rename(columns={currMS: 'VerseNo-Norm.'})
    return newDF

def get_order() -> pd.DataFrame:
    orderPD = pd.read_excel("./data/latMat/verseorder.xlsx", index_col=None)
    orderPD = orderPD.astype(str).replace('\.0', '', regex=True)
    orderPD.dropna(inplace=True)
    return orderPD


def latin_neofyier(infile):
    nodeDF = pd.DataFrame(columns=['NodeID', 'NodeLabels', 'NodeProps'])
    edgeDF = pd.DataFrame(columns=['FromNode', 'ToNode', 'EdgeLabels', 'EdgeProps', 'HRF'])
    latDict = latin_para_import(infile, outputDF="dict")
    msDict = {'B1': 'rx46', 'P3': 'rx9', 'To': 'rx51', 'W1': 'rx24'}
    edgeHelperDict2 = {}
    for key, value in msDict.items():
        updog = {key: value}
        edgeHelperDict2.update(updog)
        # nodeDF = nodeDF.append({'NodeID': f"'{value}'",  # TODO: Check if needed
        #                         'NodeLabels': f'{TXTWITDES}',
        #                         'NodeProps': f"InMS: '{key}'"},
        #                         ignore_index=True)
    for key, value in latDict.items():
        count1 = 0
        count2 = 1
        print(f"Now processing manuscript {key}")
        voNorm = ""
        voDipl = ""
        notFirst = False
        for index, row in value.iterrows():
            words = row['Word'].split()
            nodeDF = nodeDF.append({
                                'NodeID': f"'v{msDict[key]}-{row['Verse']}'",
                                'NodeLabels': 'ZZ1_Verse', 
                                'NodeProps': f"VerseDipl: '{row['Verse']}', VerseNorm: '{index}', inMS: '{msDict[key]}'"},
                                ignore_index=True)
            edgeDF = edgeDF.append({'FromNode': f"'v{msDict[key]}-{row['Verse']}'",
                            'ToNode': f"'{edgeHelperDict2[key]}'",
                            'EdgeLabels': 'ZZ3_VersinMS',
                            'HRF': f"{key} - {row['Verse']} to MS {msDict[key]}"},
                            ignore_index=True)
            if notFirst:
                edgeDF = edgeDF.append({'FromNode': f"'v{key}-{voDipl}'",
                                        'ToNode': f"'v{key}-{row['Verse']}'",
                                        'EdgeLabels': 'vNext_dipl',
                                        'HRF': f"{key} - Verse {voDipl} to next Verse {row['Verse']}"},
                                        ignore_index=True)
                edgeDF = edgeDF.append({'FromNode': f"'v{key}-{voNorm}'",
                                        'ToNode': f"'v{key}-{index}'",
                                        'EdgeLabels': 'vNext_norm',
                                        'HRF': f"{key} - Verse {voDipl} to next Verse {row['Verse']}"},
                                        ignore_index=True)

            notFirst = True
            voNorm = index
            voDipl = row['Verse']
            for word in words:
                nodeDF = nodeDF.append({
                                'NodeID': f"'{key}-{count2}'",
                                'NodeLabels': 'E33_Linguistic_Object', 
                                'NodeProps': f"Normalized: '{word}', VerseDipl: '{row['Verse']}', VerseNorm: '{index}', inMS: '{key}', pos: '{key}-{count2}'"},
                                ignore_index=True)
                edgeDF = edgeDF.append({'FromNode': f"'{key}-{count2}'",
                            'ToNode': f"'{edgeHelperDict2[key]}'",
                            'EdgeLabels': 'P56_Is_Found_On',
                            'HRF': f"{key} - {word} to MS {key}"},
                            ignore_index=True)
                edgeDF = edgeDF.append({'FromNode': f"'{key}-{count2}'",
                            'ToNode': f"'v{key}-{row['Verse']}'",
                            'EdgeLabels': 'ZZ2_inVerse',
                            'HRF': f"{key} - {word} to MS {key}"},
                            ignore_index=True)
                if count1 > 0:
                    edgeDF = edgeDF.append({'FromNode': f"'{key}-{count1}'",
                                                        'ToNode': f"'{key}-{count2}'",
                                                        'EdgeLabels': 'next',
                                                        'HRF': f"{key} - {word} to next"},
                                                        ignore_index=True)
                count1 +=1
                count2 +=1
    return nodeDF, edgeDF


def parse(inFile):
    return read_latin_xml(inFile)