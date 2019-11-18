from py2neo import Graph, Node, Relationship, Database
from bs4 import BeautifulSoup
import lxml
import time

#Get to the chopper! (choper = graphDB)

db = Database()
passwd = input("Input password for the graph you are trying to connect to")
myGraph = Graph(host="localhost", password="{}".format(passwd))
delAll = input("Do you want to clear the database before you continue? Warning! All data will be deleted! Y/N?:")
if delAll == "Y":
    myGraph.delete_all()
    print("All data deleted!")
else:
    print("OK! No data deleted.")
time.sleep(1)

#Get our stuff from somewhere and cook some soup.

fileName = input("Specify file name:")
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
nodeLabel = input("What label(s) do you want to assign to the nodes to be created?:")
edgeLabel = input("How do you want to label the edges connecting the words?")
wIDprefix = input("Specify prefix for Label WordID")
#Lets get us some multiple representation layers out of this soup!

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
    myGraph.run("Create (n:%s {Normalized: '%s', Diplomatic: '%s', Lemma: '%s', WordID: '%s%d'})" % (nodeLabel, normClean, diplClean, lemming, wIDprefix, countFrom))
    myGraph.run(
        "MATCH (a:%s),(b:%s) WHERE a.WordID = '%s%d' AND b.WordID = '%s%d'  CREATE (a)-[r:%s]->(b)" % (
        nodeLabel, nodeLabel, wIDprefix, countFrom - 1, wIDprefix, countTo - 1, edgeLabel))
    countFrom += 1
    countTo += 1
    print("Node and edge created!")
print("All done. Bye!")







