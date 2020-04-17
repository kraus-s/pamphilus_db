from bs4 import BeautifulSoup
import lxml
import time

tfilename = input("Name the target!:")
target = open(tfilename, "w+")

source = input("Specify file name:")
def read_tei(tei_file):
    with open(tei_file, 'r', encoding="UTF-8") as tei:
        soup = BeautifulSoup(tei, from_encoding='UTF-8')
        return soup

soup = read_tei('{}'.format(source))
thewords = soup.findAll('w')

#def lemmingextract():
lemmings = []
for theshit in thewords:
    lemming = theshit.get('lemma')
    lemmings.append(lemming)
    target.write( "{}, ".format(lemming))

print(lemmings)
time.sleep(5)


target.close()

print("Done!")