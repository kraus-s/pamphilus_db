from py2neo import Graph, Node, Relationship, Database, NodeMatcher
from bs4 import BeautifulSoup
import lxml
from pathlib import Path
import pandas as pd
import xlrd

# Prepare variables and functions.
# First off, connect to the database.
db = Database()
passwd = 12
chopper = Graph(host="localhost", password="{}".format(passwd))

# Create the TEI-reader function.
def read_tei(tei_file):
    with open(tei_file, 'r', encoding="UTF-8") as tei:
        soup = BeautifulSoup(tei, "lxml-xml", from_encoding='UTF-8')
        return soup
# Import the stop word list
with open('exclusionlist.txt', 'r', encoding="UTF-8") as exclusionlist:
  stopWords = exclusionlist.read()

# Define directory for files to be processed.
safeHouse = Path('.')

# Additional files
resultsONP = open('pamph-lemmata-cooccurrences.html', 'r', encoding="UTF-8")


# Prepare the database.
chopper.delete_all()
print("Importing ontology")
chopper.run('CREATE INDEX ON :Resource(uri)') # Required for neosemantics to work.
chopper.run('CREATE INDEX ON :MTX3_Word_On_Page(WordID)') #Increased speed by so much, the SSD can't keep up anymore. Jeez.
chopper.run('CREATE INDEX ON :MTX2_Textual_Entity(Name)')
chopper.run('CREATE INDEX ON :MTX3_Textual_Entity(TXP4_Composes)')
chopper.run(
    'CALL semantics.importOntology("https://raw.githubusercontent.com/svenakin/ONO/master/ontx-current.rdfs", "RDF/XML")')

# Making a bowl of contextual historical data for our soup.
# Importing the table of relevant historical persons
#


# Now that we have a bowl, we can fill it with some soup
for ymir in safeHouse.glob(("*.xml")):
    fileName = (ymir)
    print("Now opening file {}".format(fileName))

    # Building the soup, extracting information and storing it in variables.
    soup = read_tei('{}'.format(fileName))
    thewords = soup.findAll('w')
    streetName = soup.find("msName")
    friendlyName = streetName.get_text()  # This is the name of the text work stored in the XML
    getIdno = soup.find("idno")
    msSignature = getIdno.get_text()  # This is the shelfmark, signature or other identifier of the physical manuscript
    nodeLabel = "MTX3_Word_On_Page"
    edgeLabel = "next"

    # Create MS and Text
    msNode = Node("MTX1_Text_Carrier", Signature="%s" % (msSignature))
    chopper.run("CREATE (a:MTX2_Textual_Entity {Name: '%s'})" % (friendlyName))
    if chopper.exists(msNode) == False:
        chopper.run("CREATE (a:MTX1_Text_Carrier {Signature: '%s'})" % (msSignature))
    else:
        pass
    chopper.run("MATCH (a:MTX1_Text_Carrier), (b:MTX2_Textual_Entity) WHERE a.Signature = '%s' and b.Name = '%s' CREATE (a)-[r:P46_is_composed_of]->(b)" % (msSignature, friendlyName))


    # Lets get us some multiple representation layers out of this soup!

    countTotal = len(thewords)
    countFrom = 0
    countTo = 1
    for theshit in thewords:
        lemming = theshit.get('lemma')
        if lemming is not None:
            lemming = theshit.get('lemma')
        else:
            lemming = "-"
        diplRaw = theshit.find('me:dipl')
        if diplRaw is not None:
            diplClean = diplRaw.get_text()
        else:
            diplClean = "-"
        normRaw = theshit.find('me:norm')
        if normRaw is not None:
            normClean = normRaw.get_text()
        else:
            normClean = "-"
        chopper.run( 'Create (n:%s {Normalized: "%s", Diplomatic: "%s", Lemma: "%s", WordID: "%s-%d", TXP4_Composes:"%s"})' % (nodeLabel, normClean, diplClean, lemming, friendlyName, countFrom, friendlyName))
        chopper.run( 'MATCH (a:%s), (b:%s) WHERE a.WordID = "%s-%d" AND b.WordID = "%s-%d"  CREATE (a)-[r:%s]->(b)' % (nodeLabel, nodeLabel, friendlyName, countFrom - 1, friendlyName, countTo - 1, edgeLabel))
        countFrom += 1
        countTo += 1
        print("Node and edge created for {}!".format(friendlyName))
    chopper.run("MATCH (a:MTX3_Word_On_Page), (b:MTX2_Textual_Entity) WHERE a.TXP4_Composes = '%s' and b.Name = '%s' CREATE (a)-[r:TXP4_Composes]->(b)" % (friendlyName, friendlyName))

# Adding what we got from ONP



print("Moving on, connecting the lemmata.")
chopper.run("Match (a:MTX3_Word_On_Page), (b:MTX3_Word_On_Page) WHERE NOT a.Lemma IN IN [%s] AND a.Lemma = b.Lemma CREATE (a)-[r:MTP1_Also_Occurs_In]->(b)" % (stopWords))

print("All done. Bye!")
