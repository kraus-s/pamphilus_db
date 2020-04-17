from py2neo import Graph, Node, Relationship, Database
from bs4 import BeautifulSoup
import lxml
import time

#Get to the chopper! (chopper = graphDB)

db = Database()
passwd = input("Input password for the graph you are trying to connect to")
myGraph = Graph(host="localhost", password="{}".format(passwd))
myGraph.delete_all()


#Get our stuff from somewhere and cook some soup.

fileName = 'DG-4at7-Pamph.xml'
    # input("Specify file name:")
def read_tei(tei_file):
    with open(tei_file, 'r', encoding="UTF-8") as tei:
        soup = BeautifulSoup(tei, from_encoding='UTF-8')
        return soup

soup = read_tei('{}'.format(fileName))
thewords = soup.findAll('w')
normalized = soup.findAll('me:norm')
facsimile = soup.findAll('me:facs')
diplomatic = soup.findAll('me:dipl')
lemmatized = soup.get('lemma')


print("We need to name our data!")
nodeLabel = "MTX3_Word_On_Page"
edgeLabel = input("How do you want to label the edges connecting the words?")
wIDprefix = input("Specify prefix for Label WordID")
propInText = input("Specify name of textual entity to be used as TXP4_composes property:")
#Lets get us some multiple representation layers out of this soup!
myGraph.run("CREATE (a:MTX2_Textual_Entity {Name: 'Pamphilus saga'})")

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
    myGraph.run('Create (n:%s {Normalized: "%s", Diplomatic: "%s", Lemma: "%s", WordID: "%s-%d", TXP4_Composes:"Pamphilus saga"})' % (nodeLabel, normClean, diplClean, lemming, wIDprefix, countFrom))
    myGraph.run(
        "MATCH (a:%s),(b:%s) WHERE a.WordID = '%s%d' AND b.WordID = '%s%d'  CREATE (a)-[r:%s]->(b)" % (
        nodeLabel, nodeLabel, wIDprefix, countFrom - 1, wIDprefix, countTo - 1, edgeLabel))
    countFrom += 1
    countTo += 1
    print("Node and edge created!")



print("All done. Bye!")







