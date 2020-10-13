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
wordonpageBamboo = pd.DataFrame(columns=['PositionalID', 'Lemma', 'Diplomatic', 'Normalized'])
wordconnectorBamboo = pd.DataFrame(columns=['WordFirst', 'WordNext'])
word2txtwitBamboo = pd.DataFrame(columns=['PositionalID', 'TxtWitName'])
peopleBamboo = pd.DataFrame(columns=["Name", "Year Born", "Year Died", "URI"])
authBamboo = pd.DataFrame(columns=["TextFamily", "URI"])

# Create the TEI-reader function.
def read_tei(tei_file):
    with open(tei_file, 'r', encoding="UTF-8") as tei:
        soup = BeautifulSoup(tei, "lxml-xml", from_encoding='UTF-8')
        return soup

def menotaParse(inFile):
    soup = read_tei('{}'.format(inFile))
    thewords = soup.findAll('w')
    msInfo = soup.find("sourceDesc")
    msAbbreviation = msInfo.msDesc.idno.get_text()  # This is the shelfmark, signature or other identifier of the physical manuscript
    friendlyName = msInfo.msDesc.msName.get_text() # Name of the text witness, not the MS itself misleadingly

    # Throwing MS and witness to the pandas
    currmsbamboo = pd.DataFrame({'Abbreviation': [msAbbreviation]})
    currtxtwitbamboo = pd.DataFrame({'Name': [friendlyName]})
    currmapbamboo = pd.DataFrame({'MSID': [msAbbreviation], 'TXTID': [friendlyName]})

    msBamboo.append(currmsbamboo)
    txtwitBamboo.append(currtxtwitbamboo)
    text2msBamboo.append(currmapbamboo)

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

        currentBamboo = pd.DataFrame({
            'PositionalID': [currSource],
            'Lemma': [lemming],
            'Diplomatic': [diplClean],
            'Normalized': [normClean],
            'InText': [friendlyName]
        })

        currentBamboo1 = pd.DataFrame({
            'WordFirst': [currSource],
            'WordNext': [currTarget]
        })

        currentBamboo2 = pd.DataFrame({
            'PositionalID': [currSource],
            'TxtWitName': [friendlyName]
        })

        wordonpageBamboo.append(currentBamboo)
        wordconnectorBamboo.append(currentBamboo1)
        word2txtwitBamboo.append(currentBamboo2)
        countFrom += 1
        countTo += 1
    print("Fuck")
    return

def runParse(inFiles):
    results = ThreadPool(2).imap(menotaParse, inFiles)
    return

# Import the stop word list
with open('stopwords.txt', 'r', encoding="UTF-8-sig") as exclusionlist:
    stopWords = exclusionlist.read().split(',')


# Define directory for files to be processed.
safeHouse = Path('.')

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


txtwitInfobamboo = pd.DataFrame(bowlIn1, columns=['Name'])
txtwitBamboo = txtwitBamboo.append(txtwitInfobamboo)

interim_txtwit2msmappingBamboo = pd.DataFrame(bowlIn1, columns=['Signature', 'Name'])
interim_txtwit2msmappingBamboo.rename(columns={0: 'MSID', 1: 'TXTID'}, inplace=True)
text2msBamboo.append(interim_txtwit2msmappingBamboo)

currAuthbamboo = pd.DataFrame(bowlIn1, columns=['TextFamily', 'AuthURI'])
authBamboo.append(currAuthbamboo)

currPeoplebamboo = pd.DataFrame(bowlin2, columns=['Name', 'URI'])
peopleBamboo.append(currPeoplebamboo)

print("Processed Excel-sheets!")
# Now that we have a bowl, we can fill it with some soup

for ymir in safeHouse.glob(("*.xml")):
    fileName = (ymir)
    menotaParse(ymir)
    print("Done with one MS!")

# Creating the edge list for the lemma matching

samediffbamboo = wordonpageBamboo[~wordonpageBamboo['Lemma'].isin(stopWords)]


# Feeding it into the graph
for index, row in wordonpageBamboo.iterrows():
    chopper.run('''
    MERGE(a:E33_Linguistic_Object{Lemma:$Lemma, Diplomatic:$Diplomatic, Normalized:$Normalized, WordID:$PositionalID, InText:$Name})
    ''', parameters = {'Lemma': row['Lemma'], 'Diplomatic': row['Diplomatic'], 'Normalized': row['Normalized'], 'PositionalID': row['PositionalID'], 'Name': row['InText']})
print("Word on page done!")

# Here go the manuscripts!
for index, row in msBamboo.iterrows():
    chopper.run('''
        MERGE(a:E18_Physical_Thing{Abbreviation:$Abbreviation})
        ''', parameters={'Abbreviation': row['Abbreviation']})
print("MSs done!")

tx = chopper.begin() # Here go the text witnesses aka the physically written texts on the individual parchment leaves
for index, row in txtwitBamboo.iterrows():
    tx.evaluate('''
            MERGE(a:TX1_Written_Text{Name:$Name})
            ''', parameters={'Name': row['Name']})
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
                    WHERE a.Name = $Name AND b.Abbreviation = Abbreviation
                    CREATE (b)-[r:P56_Is_Found_On]->(a)
                    ''', parameters={'Name': row['TXTID'], 'Abbreviation': row['MSID']})
tx.commit()
print("Witnesses linked to MSs!")


for index, row in word2txtwitBamboo.iterrows():
    chopper.run('''
                        MATCH (a:TX1_Written_Text), (b:E33_Linguistic_Object)
                        WHERE a.Name = $Name AND b.WordID = $WordID
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


print("All done. Bye!")
