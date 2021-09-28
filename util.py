from bs4 import BeautifulSoup
import pandas as pd
from functools import reduce as red



def read_tei(tei_file) -> BeautifulSoup:
    """Takes a file path as input and returns a beautifulsoup object."""
    with open(tei_file, 'r', encoding="UTF-8") as tei:
        soup = BeautifulSoup(tei, "lxml-xml", from_encoding='UTF-8')
        return soup


# Latin functions
#################


def read_latin_xml(infile):
    soup = read_tei(infile)
    verses = soup.find_all('v')
    witnessB1Bamboo = pd.DataFrame(columns=['Verse', 'Word'])
    witnessP3Bamboo = pd.DataFrame(columns=['Verse', 'Word'])
    witnessToBamboo = pd.DataFrame(columns=['Verse', 'Word'])
    witnessW1Bamboo = pd.DataFrame(columns=['Verse', 'Word'])
  
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
        newDF = df_obj.apply(lambda x: x.str.strip())
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