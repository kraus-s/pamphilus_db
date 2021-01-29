from py2neo import Graph
from bs4 import BeautifulSoup
import lxml
from pathlib import Path
import pandas as pd
import xlrd
from multiprocessing.pool import ThreadPool


# Dataframe hatching chamber

msBamboo = pd.DataFrame(columns=['Abbreviation', 'Signature', 'Current Location'])
text2msBamboo = pd.DataFrame(columns=['MSID', 'TXTID'])
txtwitBamboo = pd.DataFrame(columns=['Name', 'Signature'])
wordonpageBamboo = pd.DataFrame(columns=['PositionalID', 'Lemma', 'Diplomatic', 'Normalized', 'InText'])
wordconnectorBamboo = pd.DataFrame(columns=['WordFirst', 'WordNext'])
word2txtwitBamboo = pd.DataFrame(columns=['PositionalID', 'TxtWitName'])
peopleBamboo = pd.DataFrame(columns=["Name", "Year Born", "Year Died", "URI"])
authBamboo = pd.DataFrame(columns=["TextFamily", "URI"])
verseParaEdgeListBamboo = pd.DataFrame(columns=['UniqueVerseID', 'PositionalID'])

pd.set_option("display.max_rows", None, "display.max_columns", None)

# Create the TEI-reader function.
def read_tei(tei_file):
    with open(tei_file, 'r', encoding="UTF-8") as tei:
        soup = BeautifulSoup(tei, "lxml-xml", from_encoding='UTF-8')
        return soup

def regmenotaParse(inFile):

    soup = read_tei('{}'.format(inFile))
    thewords = soup.findAll('w')
    msInfo = soup.find("sourceDesc")
    msAbbreviation = msInfo.msDesc.idno.get_text()  # This is the shelfmark, signature or other identifier of the physical manuscript
    msAbbreviation = msAbbreviation.replace(" ", "")
    friendlyName = msInfo.msDesc.msName.get_text() # Name of the text witness, not the MS itself misleadingly
    properIdentifier = msAbbreviation+"-"+friendlyName.replace(" ", "")

    # Throwing MS and witness to the pandas
    currmsbamboo = pd.DataFrame({'Abbreviation': [msAbbreviation]})
    currtxtwitbamboo = pd.DataFrame({'Name': friendlyName, 'UniqueID': properIdentifier})
    currmapbamboo = pd.DataFrame({'MSID': [msAbbreviation], 'TXTID': properIdentifier})
    currentBamboo = pd.DataFrame(columns=['PositionalID', 'Lemma', 'Diplomatic', 'Normalized', 'InText'])
    currentBamboo1 = pd.DataFrame(columns=['WordFirst', 'WordNext'])
    currentBamboo2 = pd.DataFrame(columns=['PositionalID', 'TxtWitName'])



    # Lets get us some multiple representation layers out of this soup!
    countFrom = 0
    countTo = 1

    for realtalk in thewords:

        currSource = "{}-{}".format(friendlyName, countFrom)
        currTarget = "{}-{}".format(friendlyName, countTo)

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

        currentBambooFeeder = pd.DataFrame({
            'PositionalID': [currSource],
            'Lemma': [lemming],
            'Diplomatic': [diplClean],
            'Normalized': [normClean],
            'InText': [friendlyName]
        })


        currentBamboo1Feeder = pd.DataFrame({
            'WordFirst': [currSource],
            'WordNext': [currTarget]
        })

        currentBamboo2Feeder = pd.DataFrame({
            'PositionalID': [currSource],
            'TxtWitName': [friendlyName]
        })

        currentBamboo = currentBamboo.append(currentBambooFeeder, ignore_index= True)
        currentBamboo1 = currentBamboo1.append(currentBamboo1Feeder, ignore_index= True)
        currentBamboo2 = currentBamboo2.append(currentBamboo2Feeder, ignore_index= True)

        countFrom += 1
        countTo += 1
    print("Done with" + msAbbreviation)
    return currmsbamboo, currmapbamboo, currtxtwitbamboo, currentBamboo, currentBamboo1, currentBamboo2

def paramenotaParse(inFile):
    soup = read_tei('{}'.format(inFile))
    paraVerses = soup.findAll("para")
    msInfo = soup.find("sourceDesc")
    msAbbreviation = msInfo.msDesc.idno.get_text()  # This is the shelfmark, signature or other identifier of the physical manuscript
    msAbbreviation = msAbbreviation.replace(" ", "")
    friendlyName = 'DG4-7-Pamph' # Name of the text witness, not the MS itself misleadingly
    properIdentifier = friendlyName

    # Throwing MS and witness to the pandas
    currmsbamboo = pd.DataFrame({'Abbreviation': [msAbbreviation]})
    currtxtwitbamboo = pd.DataFrame({'Name': [friendlyName], 'UniqueID': [properIdentifier]})
    currmapbamboo = pd.DataFrame({'MSID': [msAbbreviation], 'TXTID': properIdentifier})
    currentBamboo = pd.DataFrame(columns=['PositionalID', 'Lemma', 'Diplomatic', 'Normalized', 'InText'])
    currentBamboo1 = pd.DataFrame(columns=['WordFirst', 'WordNext'])
    currentBamboo2 = pd.DataFrame(columns=['PositionalID', 'TxtWitName'])
    versifyparallelizerUltimateBamboo3000 = pd.DataFrame(columns=['UniqueVerseID', 'PositionalID'])


    countFrom = 0
    countTo = 1
    for indiVerse in paraVerses:
        
        currVerse = indiVerse.get('vn')
        variantBecker = indiVerse.get('var')
        if variantBecker is None:
            print("No parallelization")
        elif variantBecker == "x":
            print("No para")
        else:
            print(currVerse+variantBecker)
            if variantBecker == "a":
                variantList = ['B1', 'P3', 'To', 'W1']
            else:
                variantList = [element.replace(" ", "") for element in variantList]
                variantList = variantBecker.split(',')
            if "," in currVerse:
                currVerse = currVerse.replace(" ", "")
                currVerse1, currVerse2 = currVerse.split(",")
                uniqueVerseID1 = ['{}-{}'.format(varID, currVerse1) for varID in variantList]
                uniqueVerseID2 = ['{}-{}'.format(varID, currVerse2) for varID in variantList]
                multipleVerseParallelization = 1
            else:
                uniqueVerseID = ['{}-{}'.format(varID, currVerse) for varID in variantList]
                multipleVerseParallelization = 0

            words2beParallelized = indiVerse.findAll('w')
            for realtalk in words2beParallelized:

                currSource = "{}-{}".format(properIdentifier, countFrom)
                currTarget = "{}-{}".format(properIdentifier, countTo)

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

                currentBambooFeeder = pd.DataFrame({
                    'PositionalID': [currSource],
                    'Lemma': [lemming],
                    'Diplomatic': [diplClean],
                    'Normalized': [normClean],
                    'InText': [friendlyName]
                })


                currentBamboo1Feeder = pd.DataFrame({
                    'WordFirst': [currSource],
                    'WordNext': [currTarget]
                })

                currentBamboo2Feeder = pd.DataFrame({
                    'PositionalID': [currSource],
                    'TxtWitName': [friendlyName]
                })

                if multipleVerseParallelization == 0:
                    paraDict = {"UniqueVerseID":uniqueVerseID, "PositionalID":currSource}
                elif multipleVerseParallelization == 1:
                    paraDict = {"UniqueVerseID":uniqueVerseID1, "PositionalID":currSource}
                    paraDict1 = {"UniqueVerseID":uniqueVerseID2, "PositionalID":currSource}
                    paraDict.update(paraDict1)

                currentBamboo = currentBamboo.append(currentBambooFeeder, ignore_index= True)
                currentBamboo1 = currentBamboo1.append(currentBamboo1Feeder, ignore_index= True)
                currentBamboo2 = currentBamboo2.append(currentBamboo2Feeder, ignore_index= True)
                versifyparallelizerUltimateBamboo3000 = versifyparallelizerUltimateBamboo3000.append(paraDict, ignore_index=True)

                countFrom += 1
                countTo += 1
    print("Done with this shit")
    return currmsbamboo, currmapbamboo, currtxtwitbamboo, currentBamboo, currentBamboo1, currentBamboo2, versifyparallelizerUltimateBamboo3000

def robustMenota(inFile):
    soup = read_tei(inFile)
    paraDetect = soup.teiHeader.get('Para')
    if paraDetect == 'Para':
        currmsbamboo, currmapbamboo, currtxtwitbamboo, currentBamboo, currentBamboo1, currentBamboo2, versifyparallelizerUltimateBamboo3000 = paramenotaParse(soup)
    else:
        currmsbamboo, currmapbamboo, currtxtwitbamboo, currentBamboo, currentBamboo1, currentBamboo2 = regmenotaParse(soup)
    return currmsbamboo, currmapbamboo, currtxtwitbamboo, currentBamboo, currentBamboo1, currentBamboo2, versifyparallelizerUltimateBamboo3000


def runParse(inFiles):
    currmsbamboo, currmapbamboo, currtxtwitbamboo, currentBamboo, currentBamboo1, currentBamboo2 = ThreadPool(4).imap(robustMenota, inFiles)
    return currmsbamboo, currmapbamboo, currtxtwitbamboo, currentBamboo, currentBamboo1, currentBamboo2


def parsePamph1(inFile):
    soup=read_tei(inFile)
    verses = soup.findAll("v")
    properIDSuff = "Pamph"
    friendlyName = "Pamphilus"
    msList = ['B1', 'P3', 'To', 'W1']
    
    witnessB1Bamboo = pd.DataFrame(columns=['Verse', 'UniqueVID', 'Word', 'PositionalWID', 'InText'])
    witnessP3Bamboo = pd.DataFrame(columns=['Verse', 'UniqueVID', 'Word', 'PositionalWID', 'InText'])
    witnessToBamboo = pd.DataFrame(columns=['Verse', 'UniqueVID', 'Word', 'PositionalWID', 'InText'])
    witnessW1Bamboo = pd.DataFrame(columns=['Verse', 'UniqueVID', 'Word', 'PositionalWID', 'InText'])
    currentBamboo1 = pd.DataFrame(columns=['WordFirst', 'WordNext'])
    currentBamboo2 = pd.DataFrame(columns=['PositionalID', 'TxtWitName'])
    currmsbamboo = pd.DataFrame({'Abbreviation': [msList]})
    currmsbamboo = currmsbamboo.explode('Abbreviation')
    currtxtwitbamboo = pd.DataFrame(columns=['Name', 'UniqueID'])
    for element in msList:
        currtxtwitbamboo = currtxtwitbamboo.append({'Name': 'Pamphilus', 'UniqueID': "{}{}".format(element, properIDSuff)}, ignore_index=True)
    currmapbamboo = pd.DataFrame(columns= ['MSID', 'TXTID'])
    for element in msList:
        currmapbamboo = currmapbamboo.append({'MSID': element, 'TXTID': "{}{}".format(element, properIDSuff)}, ignore_index=True)
    
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
                variantList = ['B1', 'P3', 'To', 'W1']
                witnessB1Bamboo = witnessB1Bamboo.append({'Verse': currVerse, 'UniqueVID': "B1-{}".format(currVerse), 'Word': actualWord, 'PositionalWID':"B1-{}".format(countFrom), 'InText': "Pamph-B1"}, ignore_index=True)
                witnessP3Bamboo = witnessP3Bamboo.append({'Verse': currVerse, 'UniqueVID': "P3-{}".format(currVerse), 'Word': actualWord, 'PositionalWID':"P3-{}".format(countFrom), 'InText': "Pamph-P3"}, ignore_index=True)
                witnessToBamboo = witnessToBamboo.append({'Verse': currVerse, 'UniqueVID': "To-{}".format(currVerse), 'Word': actualWord, 'PositionalWID':"To-{}".format(countFrom), 'InText': 'Pamph-To'}, ignore_index=True)
                witnessW1Bamboo = witnessW1Bamboo.append({'Verse': currVerse, 'UniqueVID': "W1-{}".format(currVerse), 'Word': actualWord, 'PositionalWID':"W1-{}".format(countFrom), 'InText': 'Pamph-W1'}, ignore_index=True)
                for element in variantList:
                    currSource = "{}-{}".format(element, countFrom)
                    currTarget = "{}-{}".format(element, countTo)
                    currentBamboo1 = currentBamboo1.append({'WordFirst':currSource, 'WordNext': currTarget}, ignore_index=True)
                    currentBamboo2 = currentBamboo2.append({'PositionalID': currSource, 'TxtWitName': "{}-{}".format(element, properIDSuff)}, ignore_index=True)
            else:
                for indiVari in variants:
                    variantesConcretes = indiVari.get('variants')
                    actualWord = indiVari.get_text()
                    if "B1" in variantesConcretes:
                        currSource = "B1-{}".format(countFrom)
                        currTarget = "B1-{}".format(countTo)
                        currentBamboo1 = currentBamboo1.append({'WordFirst':currSource, 'WordNext': currTarget}, ignore_index=True)
                        witnessB1Bamboo = witnessB1Bamboo.append({'Verse': currVerse, 'UniqueVID': "B1-{}".format(currVerse), 'Word': actualWord, 'PositionalWID':"B1-{}".format(countFrom), 'InText': "Pamph-B1"}, ignore_index=True)
                        currentBamboo2 = currentBamboo2.append({'PositionalID': currSource, 'TxtWitName': "B1-{}".format(properIDSuff)}, ignore_index=True)
                    if "P3" in variantesConcretes:
                        currSource = "P3-{}".format(countFrom)
                        currTarget = "P3-{}".format(countTo)
                        currentBamboo1 = currentBamboo1.append({'WordFirst':currSource, 'WordNext': currTarget}, ignore_index=True)
                        witnessP3Bamboo = witnessP3Bamboo.append({'Verse': currVerse, 'UniqueVID': "P3-{}".format(currVerse), 'Word': actualWord, 'PositionalWID':"P3-{}".format(countFrom), 'InText': "Pamph-P3"}, ignore_index=True)
                        currentBamboo2 = currentBamboo2.append({'PositionalID': currSource, 'TxtWitName': "P3-{}".format(properIDSuff)}, ignore_index=True)
                    if "To" in variantesConcretes:
                        currSource = "To-{}".format(countFrom)
                        currTarget = "To-{}".format(countTo)
                        currentBamboo1 = currentBamboo1.append({'WordFirst':currSource, 'WordNext': currTarget}, ignore_index=True)
                        witnessToBamboo = witnessToBamboo.append({'Verse': currVerse, 'UniqueVID': "To-{}".format(currVerse), 'Word': actualWord, 'PositionalWID':"To-{}".format(countFrom), 'InText': 'Pamph-To'}, ignore_index=True)
                        currentBamboo2 = currentBamboo2.append({'PositionalID': currSource, 'TxtWitName': "To-{}".format(properIDSuff)}, ignore_index=True)
                    if "W1" in variantesConcretes:
                        currSource = "W1-{}".format(countFrom)
                        currTarget = "W1-{}".format(countTo)
                        currentBamboo1 = currentBamboo1.append({'WordFirst':currSource, 'WordNext': currTarget}, ignore_index=True)
                        witnessW1Bamboo = witnessW1Bamboo.append({'Verse': currVerse, 'UniqueVID': "W1-{}".format(currVerse), 'Word': actualWord, 'PositionalWID':"W1-{}".format(countFrom), 'InText': 'Pamph-W1'}, ignore_index=True)
                        currentBamboo2 = currentBamboo2.append({'PositionalID': currSource, 'TxtWitName': "W1-{}".format(properIDSuff)}, ignore_index=True)
            countFrom += 1
            countTo += 1

            

            
            
        
    return witnessB1Bamboo, witnessP3Bamboo, witnessToBamboo, witnessW1Bamboo, currentBamboo1, currentBamboo2, currmsbamboo, currmapbamboo, currtxtwitbamboo




# Import the stop word list
with open('stopwords.txt', 'r', encoding="UTF-8-sig") as exclusionlist:
    stopWords = exclusionlist.read().split(',')


# Define directories for files to be processed.
safeHouse = Path('.')
latMat = Path('latMat/')
norseMat = Path('norseMat/')
bowlMat = Path('bowlMat/')
paraMat = Path('paraMat/')

# Additional files
# resultsONP = open('rawfiles/pamph-lemmata-cooccurrences.html', 'r', encoding="UTF-8")

# Prepare the database.
remoteDB = "philhist-sven-1.philhist.unibas.ch"
localDB = "localhost"

# Select DB
dbSelect = input("Connect to local or remote DB? 1: Local; 2: Remote")
if dbSelect == "1":
    selectedDB = localDB
elif dbSelect == "2":
    selectedDB = remoteDB
else:
    print("Invalid option!")

# Connect to selected db
passwd = input("Password for selected graph DB:")
chopper = Graph(host=selectedDB, password=passwd)

# Clear DB for rebuild or just update some stuff?
delShit = input("Delete all or keep going? 1: Clear DB; 2: Keep stuff")
if delShit == "1":
    chopper.delete_all()
    print("Importing ontology")
    try:
        chopper.run('CREATE INDEX ON :Resource(uri)')  # Required for neosemantics to work.
    except:
        print('Shut up')
    try:
        chopper.run(
        'CREATE INDEX ON :E33_Linguistic_Object(WordID)')  # Increased speed by so much, the SSD can't keep up anymore. Jeez.
    except:
        print('Shut up')
    try:
        chopper.run('CREATE INDEX ON :TX1_Written_Text(Name)')
    except:
        print('Shut up')
    try:
        chopper.run('CREATE INDEX ON :TX1_Written_Text(P56_Is_Found_On)')
    except:
        print('Shut up')
    try:
        chopper.run(
            'CREATE INDEX ON :E33_Linguistic_Object(Lemma)')
    except:
        print('Shut up')
    try:
        chopper.run(
            '''CREATE CONSTRAINT [No_MS_Twice] [IF NOT EXISTS]
            ON (n:E18_Physical_Thing)
            ASSERT n.Abbreviation IS UNIQUE'''
        )
    except:
        print("Shut up")
    try:
        chopper.run(
        'CALL n10s.onto.import.fetch("https://raw.githubusercontent.com/Akillus/CRMtex/master/CRMtex_v1.0.rdfs", "RDF/XML")')
    except:
        print('Shut up')




# Making a bowl of contextual historical data for our soup.
# Importing different manuscripts, their contained textworks and information about authors, if any.
bowlIn1 = pd.read_excel("hss-txtwtns.xlsx")
bowlin2 = pd.read_excel("pers.xlsx")
print("Read Excel-sheets!")
msInfoBamboo = pd.DataFrame(bowlIn1, columns=['Abbreviation', 'Signature', 'Current Location'])
msInfoBamboo.drop_duplicates()
msBamboo = msBamboo.append(bowlIn1)


txtwitInfobamboo = pd.DataFrame(bowlIn1, columns=['Name', 'Signature', 'TXTID'])
txtwitBamboo = txtwitBamboo.append(txtwitInfobamboo)

text2msBamboo = pd.DataFrame(bowlIn1, columns=['Abbreviation', 'TXTID'])

currAuthbamboo = pd.DataFrame(bowlIn1, columns=['TextFamily', 'AuthURI'])
authBamboo = authBamboo.append(currAuthbamboo)

currPeoplebamboo = pd.DataFrame(bowlin2, columns=['Name', 'URI'])
peopleBamboo = peopleBamboo.append(currPeoplebamboo)

print("Processed Excel-sheets!")

# for ymir in norseMat.glob(("*.xml")):
#     fileName = (ymir)
#     currmsbamboo, currmapbamboo, currtxtwitbamboo, currentBamboo, currentBamboo1, currentBamboo2 = regmenotaParse(ymir)
#     msBamboo = msBamboo.append(currmsbamboo)
#     txtwitBamboo = txtwitBamboo.append(currtxtwitbamboo)
#     text2msBamboo = text2msBamboo.append(currmapbamboo)
#     wordonpageBamboo = wordonpageBamboo.append(currentBamboo)
#     wordconnectorBamboo = wordconnectorBamboo.append(currentBamboo1)
#     word2txtwitBamboo = word2txtwitBamboo.append(currentBamboo2)
#     print("Done with regular MENTOA files")

for ymir in paraMat.glob(("*.xml")):
    fileName = (ymir)
    currmsbamboo, currmapbamboo, currtxtwitbamboo, currentBamboo, currentBamboo1, currentBamboo2, versifyparallelizerUltimateBamboo3000 = paramenotaParse(ymir)
    # msBamboo = msBamboo.append(currmsbamboo)
    # txtwitBamboo = txtwitBamboo.append(currtxtwitbamboo)
    # text2msBamboo = text2msBamboo.append(currmapbamboo)
    wordonpageBamboo = wordonpageBamboo.append(currentBamboo)
    wordconnectorBamboo = wordconnectorBamboo.append(currentBamboo1)
    word2txtwitBamboo = word2txtwitBamboo.append(currentBamboo2)
    versifyparallelizerUltimateBamboo3000 = versifyparallelizerUltimateBamboo3000.explode('UniqueVerseID')
    print(versifyparallelizerUltimateBamboo3000)
    input("Fuck!")
print("Done with para Files")

# This is where Pamphilus latinus lives
witnessB1Bamboo, witnessP3Bamboo, witnessToBamboo, witnessW1Bamboo, currentBamboo1, currentBamboo2, currmsbamboo, currmapbamboo, currtxtwitbamboo = parsePamph1("latMat/pamphLat.xml")
# txtwitBamboo = txtwitBamboo.append(currtxtwitbamboo)
# text2msBamboo = text2msBamboo.append(currmapbamboo)
wordconnectorBamboo = wordconnectorBamboo.append(currentBamboo1)
word2txtwitBamboo = word2txtwitBamboo.append(currentBamboo2)
verseData = [witnessB1Bamboo, witnessP3Bamboo, witnessToBamboo, witnessW1Bamboo]
verseDataBamboo = pd.concat(verseData, ignore_index=True)
verseDataBamboo = verseDataBamboo.drop(columns=['Word', 'InText'])
verseCreatorBamboo = verseDataBamboo.drop(columns=['PositionalWID']).drop_duplicates().reset_index(drop=True)
verseToWordLinkBamboo = verseDataBamboo.drop(columns=['Verse'])


# Creating the edge list for the lemma matching

samediffbamboo = wordonpageBamboo[~wordonpageBamboo['Lemma'].isin(stopWords)]



# Feeding it into the graph
for index, row in wordonpageBamboo.iterrows():
    chopper.run('''
    MERGE(a:E33_Linguistic_Object{Lemma:$Lemma, Diplomatic:$Diplomatic, Normalized:$Normalized, WordID:$PositionalID, InText:$Name})
    ''', parameters = {'Lemma': row['Lemma'], 'Diplomatic': row['Diplomatic'], 'Normalized': row['Normalized'], 'PositionalID': row['PositionalID'], 'Name': row['InText']})
print("Word on page done!")

# This makes all the magic for the latin happen
for index, row in witnessB1Bamboo.iterrows():
    chopper.run('''
    MERGE(a:E33_Linguistic_Object{Word:$Word, WordID:$PositionalID, InText:$InText})
    ''', parameters = {'Word': row['Word'], 'PositionalID': row['PositionalWID'], 'InText': row['InText']})
print("Words of B1 done!")

for index, row in witnessP3Bamboo.iterrows():
    chopper.run('''
    MERGE(a:E33_Linguistic_Object{Word:$Word, WordID:$PositionalID, InText:$InText})
    ''', parameters = {'Word': row['Word'], 'PositionalID': row['PositionalWID'], 'InText': row['InText']})
print("Words of B1 done!")

for index, row in witnessToBamboo.iterrows():
    chopper.run('''
    MERGE(a:E33_Linguistic_Object{Word:$Word, WordID:$PositionalID, InText:$InText})
    ''', parameters = {'Word': row['Word'], 'PositionalID': row['PositionalWID'], 'InText': row['InText']})
print("Words of B1 done!")

for index, row in witnessW1Bamboo.iterrows():
    chopper.run('''
    MERGE(a:E33_Linguistic_Object{Word:$Word, WordID:$PositionalID, InText:$InText})
    ''', parameters = {'Word': row['Word'], 'PositionalID': row['PositionalWID'], 'InText': row['InText']})
print("Words of B1 done!")

for index, row in verseCreatorBamboo.iterrows():
    chopper.run('''
    CREATE(a:VerseObject{VerseNo:$VNo, UniqueID:$UniqueVID})
    ''', parameters={'VNo': row['Verse'], 'UniqueVID': row['UniqueVID']})
print("All Verses created")

tx = chopper.begin()
for index, row in verseToWordLinkBamboo.iterrows():
    tx.evaluate('''
    MATCH (a:E33_Linguistic_Object), (b:VerseObject)
    WHERE a.WordID = $WordPosID AND b.UniqueID = $VID
    CREATE (a)-[r:IsInVerse]->(b)
    ''', parameters={'WordPosID': row['PositionalWID'], 'VID': row['UniqueVID']})
tx.commit()
print("Verses linked")

# Here go the manuscripts!
for index, row in msBamboo.iterrows():
    chopper.run('''
        MERGE(a:E18_Physical_Thing{Abbreviation:$Abbreviation, Siglum:$Sig, CurrentLocation:$CurrLib})
        ''', parameters={'Abbreviation': row['Abbreviation'], 'Sig': row['Signature'], 'CurrLib': row['Current Location']})
print("MSs done!")

tx = chopper.begin() # Here go the text witnesses aka the physically written texts on the individual parchment leaves
for index, row in txtwitBamboo.iterrows():
    tx.evaluate('''
            MERGE(a:TX1_Written_Text{Name:$Name, InMS:$Signature, UniqueID:$TXTID})
            ''', parameters={'Name': row['Name'], 'Signature': row['Signature'], 'TXTID': row['TXTID']})
tx.commit()
print("Witnesses done!")

for index, row in wordconnectorBamboo.iterrows():
    chopper.run('''
                MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
                WHERE a.WordID = $WordFirst AND b.WordID = $WordNext
                CREATE (a)-[r:next_word]->(b)
                ''', parameters={'WordFirst': row['WordFirst'], 'WordNext': row['WordNext']})
print("Words on page chained!")

tx = chopper.begin()
for index, row in text2msBamboo.iterrows():
    tx.evaluate('''
                    MATCH (a:TX1_Written_Text), (b:E18_Physical_Thing)
                    WHERE a.UniqueID = $ID AND b.Abbreviation = $Abbreviation
                    CREATE (b)-[r:P56_Is_Found_On]->(a)
                    ''', parameters={'ID': row['TXTID'], 'Abbreviation': row['Abbreviation']})
tx.commit()
print("Witnesses linked to MSs!")


for index, row in word2txtwitBamboo.iterrows():
    chopper.run('''
                        MATCH (a:TX1_Written_Text), (b:E33_Linguistic_Object)
                        WHERE a.UniqueID = $Name AND b.WordID = $WordID
                        CREATE (b)-[r:P56_Is_Found_On]->(a)
                        ''', parameters={'Name': row['TxtWitName'], 'WordID': row['PositionalID']})
print("Words linked to witnesses!")

for index, row in samediffbamboo.iterrows():
    chopper.run('''
        MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
        WHERE a.WordID = $WordID
        AND a.Lemma = b.Lemma
        AND NOT a.InText = b.InText
        CREATE (a)-[r:also_occurs_in]->(b)
    ''', parameters={'WordID': row['PositionalID']})
print("Same same but different done!")

# Parallelize the Verses

for index, row in versifyparallelizerUltimateBamboo3000.iterrows():
    chopper.run('''
    MATCH (a:E33_Linguistic_Object), (b:VerseObject)
    WHERE a.WordID = $PosID AND b.UniqueID = $VID
    CREATE (a)-[r:was_translated_from]->(b)
    ''', parameters={'PosID': row['PositionalID'], 'VID': row['UniqueVerseID']})
print("Parallelization done!")

print("All done. Bye!")
