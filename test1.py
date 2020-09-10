from bs4 import BeautifulSoup
import lxml
import time
from pathlib import Path
import pandas as pd
from py2neo import Graph, Node, Relationship, Database, NodeMatcher
db = Database()
passwd = 12
chopper = Graph(host="localhost", password="{}".format(passwd))
matcher = NodeMatcher(chopper)

# Prepare the database.
chopper.delete_all()
print("Importing ontology")
#chopper.run('CREATE INDEX ON :Resource(uri)') # Required for neosemantics to work.
#chopper.run('CREATE INDEX ON :MTX3_Word_On_Page(WordID)') #Increased speed by so much, the SSD can't keep up anymore. Jeez.
#chopper.run('CREATE INDEX ON :MTX2_Textual_Entity(Name)')
#chopper.run('CREATE INDEX ON :MTX2_Textual_Entity(TXP4_Composes)')
chopper.run(
    'CALL n10s.onto.import.fetch("https://raw.githubusercontent.com/svenakin/ONO/master/ontx-current.rdfs", "RDF/XML")')

nodeLabel = "MTX3_Word_On_Page"

resultsONP = open('C:/Users/Sven/switchdrive/codestuff/PhData/oldnorse2graph/pamph-lemmata-cooccurrences.html', 'r', encoding="UTF-8")

dfs = pd.read_html(resultsONP, encoding="UTF-8")
pandaTree = dfs[0]
pandaTree['lemma'] = pandaTree['lemma'].str.strip('123456789')

# Create the object for Pamphilus, then create all lemmata for it
onpTextWitness = 'Pamphilus saga'
chopper.run("CREATE (a:MTX2_Textual_Entity {Name: '%s'})" % (onpTextWitness))
for row in pandaTree.itertuples():
    onpLemming = row.lemma
    onpTextWitness = 'Pamphilus saga'
    chopper.run(
        'Create (n:%s {Normalized: "-", Diplomatic: "-", Lemma: "%s", WordID: "-", TXP4_Composes:"%s"})' % (
            nodeLabel, onpLemming, onpTextWitness,))

# Reformatting the data frame so the data becomes easier to work with
pandaForest = pandaTree.other.str.split(',').apply(pd.Series)
pandaForest.index = pandaTree.lemma
pandaForest = pandaForest.stack().reset_index('lemma')
pandaForest.rename(columns={0: 'Text'}, inplace=True)
pandaForest[['Text', 'count']] = pandaForest['Text'].str.rsplit(' ', expand=True, n=1)
pandaForest['Text'] = pandaForest['Text'].str.strip()
pandaForest['count'] = pandaForest['count'].str.strip(' ()')
pandaForest['lemma'] = pandaForest['lemma'].str.strip('123456789')

print(pandaForest)

for row in pandaForest.itertuples():
    onpLemming = row.lemma
    onpTextWitness = row.Text
    repeatCounter = row.count
    if repeatCounter is None:
        pass
    else:
        for manyLemmings in repeatCounter:
            chopper.run(
                'Create (n:%s {Normalized: "-", Diplomatic: "-", Lemma: "%s", WordID: "-", TXP4_Composes:"%s"})' % (
                nodeLabel, onpLemming, onpTextWitness,))
            chopper.run('MERGE (a:MTX2_Textual_Entity {Name: "%s"})' % (onpTextWitness))
        

chopper.run("MATCH (a:MTX3_Word_On_Page), (b:MTX2_Textual_Entity) WHERE a.TXP4_Composes = b.Name CREATE (a)-[r:TXP4_Composes {weight: '5'}]->(b)")
chopper.run("MATCH (a:MTX3_Word_On_Page), (b:MTX2_Textual_Entity) WHERE a.TXP4_Composes = b.Name CREATE (b)-[r:TXP5_Is_Composed_Of{weight: '5'}]->(a)")

# Import the stop word list
with open('exclusionlist.txt', 'r', encoding="UTF-8") as exclusionlist:
  stopWords = exclusionlist.read()
# chopper.run("Match (a:MTX3_Word_On_Page), (b:MTX3_Word_On_Page) WHERE NOT a.Lemma IN [%s] AND a.Lemma = b.Lemma CREATE (a)-[r:MTP1_Also_Occurs_In]->(b)" % (stopWords))

print("Done")