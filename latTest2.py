from os import name, path
from cltk import NLP
from numpy import mat, result_type
from wordsorter import latMat
import pandas as pd
import csv

cltk_nlp = NLP(language="lat")

def analyzer(inText: str):
    anaDoc = cltk_nlp.analyze(text=inText)
    lems = anaDoc.lemmata
    MSA = anaDoc.pos
    return lems, MSA

def buildLemDF():
    mats = latMat(outputDF='dict')
    for witID, witDF in mats.items():
        witDF[['lemmata', 'MSA']] = witDF.apply(lambda x: analyzer(x['Word']), axis=1, result_type='expand')
        witDF.to_csv(f"{witID}-lemMSA.csv")
    return mats

if __name__ == '__main__':
    shit = buildLemDF()