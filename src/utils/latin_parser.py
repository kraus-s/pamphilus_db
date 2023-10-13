from bs4 import BeautifulSoup
import pandas as pd
from functools import reduce as red
from utils.util import read_tei
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


class latin_document:

    def __init__(self, abbreviation: str, shelfmark: str) -> None:
        self.name = abbreviation
        self.shelfmark = shelfmark
        self.verses = []
        self.ordered_verses = {}
    
    def add_verse(self, new_verse: verse):
        self.verses.append(new_verse)
    
    def order_verses(self, verse_object, verse_order):
        self.ordered_verses[verse_order] = verse_object


# Parsing
# -------


def parse_pamphilus(infile: str) -> dict[str, latin_document]:
    """This function will process the latin XML of Pamphilus and produce individual objects of the latin document class for every manuscript.
    It will return a dict of latin_document"""
    soup = read_tei(infile)
    verses = soup.find_all('v')
    B1 = latin_document(abbreviation="B1", shelfmark="Ms Hamilton 390")
    P3 = latin_document(abbreviation="P3", shelfmark="BNF cod. lat. 8430")
    W1 = latin_document(abbreviation="W1", shelfmark="cod. 303")
    To = latin_document(abbreviation="To", shelfmark="cod. 102")
    P5 = latin_document(abbreviation="P5", shelfmark="BNF cod. franc. 25405")
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
                    local_variants = indiVari.get('variants')
                    actualWord = indiVari.get_text()
                    if "'" in actualWord:
                        actualWord = actualWord.replace("'", "")
                    if '"' in actualWord:
                        actualWord = actualWord.replace('"', '')
                    if "a" in local_variants:
                        for i in currVList:
                            i.add_token(actualWord)
                    if "B1" in local_variants:
                        currVerseB1.add_token(actualWord)
                    if "P3" in local_variants:
                        currVerseP3.add_token(actualWord)
                    if "To" in local_variants:
                        currVerseTo.add_token(actualWord)
                    if "W1" in local_variants:
                        currVerseW1.add_token(actualWord)
                    if "P5" in local_variants:
                        curr_verse_p5.add_token(actualWord)
        B1.add_verse(currVerseB1)
        P3.add_verse(currVerseP3)
        To.add_verse(currVerseTo)
        W1.add_verse(currVerseW1)
        P5.add_verse(curr_verse_p5)
    res = {"B1": B1, "P3": P3, "To": To, "W1": W1, "P5": P5}
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


def read_latin_xml(infile) -> dict[str, pd.DataFrame]:
    soup = read_tei(infile)
    verses = soup.find_all('v')
    witnessB1_df = pd.DataFrame(columns=['Verse', 'Word'])
    witnessP3_df = pd.DataFrame(columns=['Verse', 'Word'])
    witnessTo_df = pd.DataFrame(columns=['Verse', 'Word'])
    witnessW1_df = pd.DataFrame(columns=['Verse', 'Word'])
    witnessP5_df = pd.DataFrame(columns=['Verse', 'Word'])
    witnessF_df = pd.DataFrame(columns=['Verse', 'Word'])
    df_dict: dict[str, pd.DataFrame] = {"B1": witnessB1_df, "P3": witnessP3_df, "To": witnessTo_df, "W1": witnessW1_df, "P5": witnessP5_df, "F": witnessF_df}
    for indiVerse in verses:
        
        currVerse = indiVerse.get('n')
        currVerseReps = []

        words2beWorked = indiVerse.findAll()
        for realtalk in words2beWorked:
            if realtalk.name == 'w':
                variants = realtalk.findAll('var')                
                for indiVari in variants:
                    local_variants = str(indiVari.get('variants')).replace(" ", "").split(",")
                    actualWord = indiVari.get_text()
                    if "'" in actualWord:
                        actualWord = actualWord.replace("'", "")
                    if '"' in actualWord:
                        actualWord = actualWord.replace('"', '')
                    if "a" in local_variants:
                        for key, df in df_dict.items():
                            df_dict[key] = df.append({'Verse': currVerse, 'Word': actualWord}, ignore_index=True)
                    else:
                        for i in local_variants:
                            df = df_dict[i]
                            df_dict[i] = df.append({'Verse': currVerse, 'Word': actualWord}, ignore_index=True)

    return df_dict


def latin_para_import(infile, outputDF: str = 'merged'):
    '''Reads the parallelized XML and returns DF.
    Args:
    outputDF: if grouped returns all witnesses in one DF, column names are the sigla of the manuscripts.
    if outputDF == 'dict' returns individual DFs for each witness '''
    df_dict = read_latin_xml(infile)
    for key, value in df_dict.items():
        df = value.groupby(['Verse'])['Word'].apply(" ".join).reset_index()
        df =  df.select_dtypes(['object'])
        df = apply_sort(df, key)
        df_dict[key] = df
    if outputDF == 'dict':
        return df_dict
    if outputDF == 'merged':
        df_list = []
        for key, value in df_dict.items():
            newDF = value.rename(columns={'Word': key})
            df_list.append(newDF)
        df_merged = red(lambda left,right: pd.merge(left,right, on=['Verse'], how='outer'), df_list)
        return df_merged


def apply_sort(source_df: pd.DataFrame, current_ms: str) -> pd.DataFrame:
    """This uses an external Excel Sheet to sort the verses of each manuscript in the way they appear in the manuscript.
        Afterwards, every verse will have to verse numbers: One reflecting the order of the manuscript and one normalized
        reflecting the verse order of Beckers edition"""
    order_pd = pd.read_excel(VERSEORDER, index_col=None, usecols=['Base', current_ms])
    order_pd[current_ms] = order_pd[current_ms].astype(str).replace('\.0', '', regex=True)
    order_pd['Base'] = order_pd['Base'].astype(int)
    order_pd.dropna(inplace=True)
    new_df = pd.merge(left=source_df, right=order_pd, left_on='Verse', right_on=current_ms, how='left')
    new_df.set_index('Base', inplace=True)
    new_df = new_df.sort_index()
    new_df = new_df.rename(columns={current_ms: 'VerseNo-Norm.'})
    return new_df


def latin_neofyier(infile):
    nodeDF = pd.DataFrame(columns=['NodeID', 'NodeLabels', 'NodeProps'])
    edgeDF = pd.DataFrame(columns=['FromNode', 'ToNode', 'EdgeLabels', 'EdgeProps', 'HRF'])
    latDict = latin_para_import(infile, outputDF="dict")
    msDict = {'B1': 'berlin_hamilton_390', 'P3': 'paris_bnf_latin_8430', 'To': 'toledo_cathedral_102-11', 'W1': 'vienna_cod_303', 'P5': 'paris_bnf_franc_25405', 'F': 'frankfurt_frag_lat_11_II'}
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