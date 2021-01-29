import pandas as pd
from bs4 import BeautifulSoup
import lxml

def read_tei(tei_file):
    with open(tei_file, 'r', encoding="UTF-8") as tei:
        soup = BeautifulSoup(tei, "lxml-xml", from_encoding='UTF-8')
        return soup


def parsePamph1(inFile):
    soup=read_tei(inFile)
    verses = soup.findAll("v")
    
    witnessB1Bamboo = pd.DataFrame(columns=['Verse', 'UniqueVID', 'Word', 'PositionalWID'])
    witnessP3Bamboo = pd.DataFrame(columns=['Verse', 'UniqueVID', 'Word', 'PositionalWID'])
    witnessToBamboo = pd.DataFrame(columns=['Verse', 'UniqueVID', 'Word', 'PositionalWID'])
    witnessW1Bamboo = pd.DataFrame(columns=['Verse', 'UniqueVID', 'Word', 'PositionalWID'])

    countFrom = 0
    countTo = 1

    for indiVerse in verses:
        
        currVerse = indiVerse.get('n')

        words2beWorked = indiVerse.findAll('w')
        for realtalk in words2beWorked:
            variants = realtalk.findAll('var')
            countVars = len(variants)
            if countVars == 1:
                variants = realtalk.find('var')
                actualWord = variants.get_text()
                witnessB1Bamboo = witnessB1Bamboo.append({'Verse': [currVerse], 'UniqueVID': ["B1-{}".format(currVerse)], 'Word': [actualWord], 'PositionalWID':["B1-{}".format(countFrom)]}, ignore_index=True)
                witnessP3Bamboo = witnessP3Bamboo.append({'Verse': [currVerse], 'UniqueVID': ["P3-{}".format(currVerse)], 'Word': [actualWord], 'PositionalWID':["P3-{}".format(countFrom)]}, ignore_index=True)
                witnessToBamboo = witnessToBamboo.append({'Verse': [currVerse], 'UniqueVID': ["To-{}".format(currVerse)], 'Word': [actualWord], 'PositionalWID':["To-{}".format(countFrom)]}, ignore_index=True)
                witnessW1Bamboo = witnessW1Bamboo.append({'Verse': [currVerse], 'UniqueVID': ["W1-{}".format(currVerse)], 'Word': [actualWord], 'PositionalWID':["W1-{}".format(countFrom)]}, ignore_index=True)
                
                    
            else:
                for indiVari in variants:
                    variantesConcretes = indiVari.get('variants')
                    actualWord = indiVari.get_text()
                    if "B1" in variantesConcretes:
                        witnessB1Bamboo = witnessB1Bamboo.append({'Verse': [currVerse], 'UniqueVID': ["B1-{}".format(currVerse)], 'Word': [actualWord], 'PositionalWID':["B1-{}".format(countFrom)]}, ignore_index=True)
                    if "P3" in variantesConcretes:
                        witnessP3Bamboo = witnessP3Bamboo.append({'Verse': [currVerse], 'UniqueVID': ["P3-{}".format(currVerse)], 'Word': [actualWord], 'PositionalWID':["P3-{}".format(countFrom)]}, ignore_index=True)
                    if "To" in variantesConcretes:
                        witnessToBamboo = witnessToBamboo.append({'Verse': [currVerse], 'UniqueVID': ["To-{}".format(currVerse)], 'Word': [actualWord], 'PositionalWID':["To-{}".format(countFrom)]}, ignore_index=True)
                    if "W1" in variantesConcretes:
                        witnessW1Bamboo = witnessW1Bamboo.append({'Verse': [currVerse], 'UniqueVID': ["W1-{}".format(currVerse)], 'Word': [actualWord], 'PositionalWID':["W1-{}".format(countFrom)]}, ignore_index=True)

            

            
            
        countFrom += 1
        countTo += 1
    return witnessB1Bamboo, witnessP3Bamboo, witnessToBamboo, witnessW1Bamboo


witnessB1Bamboo, witnessP3Bamboo, witnessToBamboo, witnessW1Bamboo = parsePamph1("latMat/pamphLat.xml")
print(witnessB1Bamboo)