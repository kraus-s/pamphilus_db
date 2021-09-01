from bs4 import BeautifulSoup
import pandas as pd
from lxml import etree
from pandas.core.frame import DataFrame
from dbBuilder import parsePamph1 as latParse
import requests
from dbBuilder import read_tei
import typing
from collections import Counter
from pathlib import Path
from functools import reduce as red


latPara = "latMat/pamphLat.xml"
onPara = "paraMat/DG-4at7-Pamph-para.xml"
outPath = "outdata/"
menotaEnt = "menota-ents-1.txt"
proielData = Path('norsemat/PROIEL/').rglob('*.htm')
pd.set_option('display.max_colwidth', None)


def paramenotaParse(inFile):
    intro = "<!DOCTYPE TEI ["
    outro = " ]>"
    with open(inFile, 'r', encoding="UTF-8") as trash:
        presoup = trash.read()
    with open(menotaEnt, 'r', encoding='UTF-8') as ents:
        treebeard = ents.read()
    pot = etree.fromstring(intro + treebeard + outro + presoup)
    
    soup = BeautifulSoup(etree.tostring(pot, encoding='UTF-8'), features='lxml')
    paraVerses = soup.findAll('para')
    # msInfo = soup.find("sourcedesc")
    # msAbbreviation = msInfo.msDesc.idno.get_text()  # This is the shelfmark, signature or other identifier of the physical manuscript
    # msAbbreviation = msAbbreviation.replace(" ", "")
    friendlyName = 'DG4-7-Pamph' # Name of the text witness, not the MS itself misleadingly
    properIdentifier = friendlyName

    # Throwing MS and witness to the pandas
    pamphBamboo = pd.DataFrame(columns=['Verse', 'Variant', 'Lemma', 'Normalized', 'Facsimile', 'MSA'])
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
            pamphBamboo = pamphBamboo.append({'Verse': currVerse, 'Variant': variantBecker, 'Lemma': lemming, 'Normalized': normClean, 'Facsimile': facsClean, 'MSA': msaClean, 'Order': counter}, ignore_index=True)
        counter += 1
    return pamphBamboo

def onMat():
    bamboozled = paramenotaParse(onPara)
    bamboozled = bamboozled.groupby(['Verse', 'Order'])['Normalized', 'Facsimile', 'MSA', 'Lemma'].agg(" ".join).reset_index()
    # simpleCount = bamboozled.groupby('Variant')['Verse'].nunique()
    # simpleCount.to_csv(f"{outPath}vars.csv")
    return bamboozled

def latMat(outputDF: str = 'merged'):
    '''Reads the parallelized XML and returns DF.
    Args:
    outputDF: if grouped returns all witnesses in one DF, column names reflecting MS abbrs.
    if outputDF == 'dict' returns individual DFs for each witness '''
    Berlin1, Paris3, Toledo, Wien1 = latParse(latPara)
    Berlin1 = Berlin1.groupby(['Verse'])['Word'].apply(" ".join).reset_index()
    Paris3 = Paris3.groupby(['Verse'])['Word'].apply(" ".join).reset_index()
    Toledo = Toledo.groupby(['Verse'])['Word'].apply(" ".join).reset_index()
    Wien1 = Wien1.groupby(['Verse'])['Word'].apply(" ".join).reset_index()
    if outputDF == 'dict':
        zoo = {"B1": Berlin1,"P3": Paris3, "To": Toledo, "W1": Wien1}
        return zoo
    if outputDF == 'merged':
        Berlin1 = Berlin1.rename(columns={'Word': 'B1'})
        Paris3 = Paris3.rename(columns={'Word': 'P3'})
        Toledo = Toledo.rename(columns={'Word': 'To'})
        Wien1 = Wien1.rename(columns={'Word': 'W1'})
        zoo = [Berlin1, Paris3, Toledo, Wien1]
        df_merged = red(lambda left,right: pd.merge(left,right, on=['Verse'], how='outer'), zoo)
        return df_merged


def syntaxAnalyser(inDF: pd.DataFrame):
    outDF = pd.DataFrame(columns=['Verse', 'Order', 'Normalized', 'MSA', 'WordOrder', 'KlausTyp'])
    inDF.drop(inDF.index[inDF["Verse"].apply(lambda x: not (x.strip().isnumeric()))], axis=0, inplace=True)
    for index, row in inDF.iterrows():
        firstOrder = list(filter(None, row['MSA'].split('x')))
        firstOrder = [x.strip() for x in firstOrder]
        wordOrder, clauseType = sentTypologizer(firstOrder)
        outDF = outDF.append({'Verse': row['Verse'], 'Order': row['Order'], 'Normalized': row['Normalized'], 'MSA': row['MSA'], 'WordOrder': wordOrder, 'KlausTyp': clauseType}, ignore_index=True)
    outDF.to_csv(f"{outPath}on-syntax.csv")
    


def sentTypologizer(inSent: typing.List):
    clauseType = 'Undefined'
    oblique = ['cA', 'cD', 'cG']
    relevantWordTypes = ['NC', 'NP', 'PE', 'DD', 'VB fF', 'DQ', 'PQ']
    if inSent[0] == 'CC' and not inSent[1] == 'CS':
        clauseType = 'Parataxe'
    if inSent[0] == 'CS':
        clauseType = 'Hypotaxe'
    if inSent[1] == 'CS':
        clauseType = 'Hypotaxe'
    wordOrder = []
    for val in inSent:
        if any(x in val for x in relevantWordTypes):
            if 'cN' in val and not 'VB' in val:
                syntFunc = 'S'
            if 'fF' in val:
                syntFunc = 'V'
            if any(x in val for x in oblique):
                syntFunc = 'O'
            try:    
                wordOrder.append(syntFunc)
            except:
                print(val)
    typeTally = Counter(wordOrder)
    if typeTally['V'] == 1 and typeTally['S'] >= 1 and typeTally['O'] >= 1:
        seen = set()
        bruteOrder = []
        for x in wordOrder:
            if x not in seen:
                bruteOrder.append(x)
                seen.add(x)
        return bruteOrder, clauseType

    
    return wordOrder, clauseType


def doPROIEL():
    allDF = pd.DataFrame(columns=['SentID', 'Token'])
    sentClean = 0
    for rawP in proielData:
        with open(rawP, 'r', encoding="UTF-8") as trash:
            presoup = trash.read()
        soup = BeautifulSoup(presoup, features='lxml')
        preFiltered = soup.find('div', class_='formatted-text')
        relevantShit = preFiltered.find_all('a')
        for token in relevantShit:
            tagType = token.get('class')
            if tagType[0] == 'reviewed':
                pass
            if tagType[0] == 'sentence-number':
                sentClean += 1
            if tagType[0] == 'token':        
                word = token.get_text()
                outDict = {'SentID': sentClean, 'Token': word}
                allDF = allDF.append(outDict, ignore_index=True)
    outDF = allDF.groupby('SentID')['Token'].apply(" ".join).reset_index()
    outDF = outDF.set_index('SentID', drop=False)
    return outDF

def sentVcomp(pamphVers: DataFrame = None):
    proielSents = doPROIEL()
    if not pamphVers:
        pamphVers = onMat()
    
    

def findUpper(inDF: pd.DataFrame):
    initList = ['1', '25', '71', '143', '153', '163', '178', '186', '193', '213', 
                '227', '238', '245', '285', '299', '313', '321', '329', '339', '381', 
                '401', '405', '421', '429', '442', '451', '463', '471', '479', '487, 488']
    outDF = pd.DataFrame(columns=['Order', 'Verse', 'Facsimile'])
    for index, row in inDF.iterrows():
        upp = False
        if row['Verse'] not in initList:
            contents = row['Facsimile']
            for chr in contents:
                if chr.isupper():
                    upp = True
            if upp:
                outDF = outDF.append({'Order': row['Order'], 'Verse': row['Verse'], 'Facsimile': row['Facsimile']}, ignore_index=True)
                print(outDF)
    outDF.to_csv(f"{outPath}on-UpperV.csv")


def latin_norse_merger(latDF: pd.DataFrame, norseDF: pd.DataFrame) -> pd.DataFrame:
    merged_DF = pd.merge(latDF, norseDF, how='outer')
    return merged_DF


def easy_bake_oven() -> pd.DataFrame:
    return latin_norse_merger(latMat(), onMat())

if __name__ == '__main__':
    shit = onMat()
    # syntaxAnalyser(shit)
    # doPROIEL()
    # sentVcomp()
    # findUpper(shit)