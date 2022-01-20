from cProfile import label
from numpy.lib.arraysetops import isin
import streamlit as st
import pandas as pd
from utils import menota_parser
from utils import latin_parser
from utils import util as ut
from utils import neo2st
import pickle
from pathlib import Path
from st_aggrid import AgGrid as ag
from annotated_text import annotated_text as at
from neo4j import GraphDatabase
from neo4j.graph import Node, Relationship
import networkx as nx
import matplotlib.pyplot as plt
from typing import List, Dict
from pyvis import network as net
from IPython.core.display import display, HTML
import streamlit.components.v1 as components


# Helper functions
# ----------------


class myData:

    oldNorse: menota_parser.paradoc

    latin: dict

    def __init__(self, on, lat) -> None:
        self.oldNorse = on
        self.latin = lat


def data_loader():
    picklePath0 = Path("data/pickleON.p")
    picklePath1 = Path("data/pickleLat.p")
    if picklePath0.is_file():
        with open("data/pickleON.p", "rb") as f:
            onPamph = pickle.load(f)
    else:
        onPamph = menota_parser.parse("data/paraMat/DG-4at7-Pamph-para.xml")
        f = open("data/pickleON.p", 'w+b')
        pickle.dump(onPamph, f)
        f.close()
    if picklePath1.is_file():
        with open("data/pickleLat.p", "rb") as f:
            latin = pickle.load(f)
    else:
        latin = latin_parser.parse("data/latMat/pamphLat.xml")
        f = open("data/pickleLat.p", 'w+b')
        pickle.dump(latin, f)
        f.close()
    return onPamph, latin


# Display functions
# -----------------


def para_display(data: myData):
    onPamph = data.oldNorse
    txtDict = data.latin
    txtDict["DG 4-7"] = data.oldNorse
    transLevel = st.selectbox("Select transcription level of Pamphilus saga", ["Diplomatic", "Normalized", "Facsimile", "Lemmatized"])
    verseSelect = st.text_input("Select Verse or Verserange")
    txtSelect = st.multiselect(label="Select witnesses", options = txtDict.keys(), default= txtDict.keys())
    colNo = len(txtSelect)
    cols = st.columns(colNo)
    lookupD = {}
    for k in txtDict.keys():
        lookupD[k] = [x.vno for x in txtDict[k].verses]
    

    for a, aa in enumerate(cols):
        aa.write(f"{txtSelect[a]}")
        currMS = txtSelect[a]
        currTxt = txtDict[currMS]

        if not verseSelect:
            if currMS == "DG 4-7":
                for v in onPamph.verses:
                    if transLevel == "Diplomatic":
                        aa.write(f"{v.vno} " + " ".join([t.diplomatic for t in v.tokens]))
                    if transLevel == "Normalized":
                        aa.write(f"{v.vno} " + " ".join([t.normalized for t in v.tokens]))
                    if transLevel == "Facsimile":
                        aa.write(f"{v.vno} " + " ".join([t.facsimile for t in v.tokens]))
                    if transLevel == "Lemmatized":
                        aa.write(f"{v.vno} " + " ".join([t.lemma for t in v.tokens]))
            else:
                for v in currTxt.verses:
                    aa.write(f"{v.vno} " + " ".join([t for t in v.tokens]))


        if "-" in verseSelect:
            i, ii = verseSelect.split("-")
            vRange = list(map(str, range(int(i), int(ii)+1)))

            if currMS == "DG 4-7":
                for v in currTxt.verses:
                    if v.vno in vRange:
                        if transLevel == "Diplomatic":
                            aa.write(f"{v.vno} " + " ".join([t.diplomatic for t in v.tokens]))
                        if transLevel == "Normalized":
                            aa.write(f"{v.vno} " + " ".join([t.normalized for t in v.tokens]))
                        if transLevel == "Facsimile":
                            aa.write(f"{v.vno} " + " ".join([t.facsimile for t in v.tokens]))
                        if transLevel == "Lemmatized":
                            aa.write(f"{v.vno} " + " ".join([t.lemma for t in v.tokens]))
                    if "," in v.vno:
                        v1 = v.vno.split(",")
                        if v1[0] in vRange:
                            if transLevel == "Diplomatic":
                                aa.write(f"{v.vno} " + " ".join([t.diplomatic for t in v.tokens]))
                            if transLevel == "Normalized":
                                aa.write(f"{v.vno} " + " ".join([t.normalized for t in v.tokens]))
                            if transLevel == "Facsimile":
                                aa.write(f"{v.vno} " + " ".join([t.facsimile for t in v.tokens]))
                            if transLevel == "Lemmatized":
                                aa.write(f"{v.vno} " + " ".join([t.lemma for t in v.tokens]))

            else:
                for v in currTxt.verses:
                    if v.vno in vRange:
                        aa.write(f"{v.vno} " + " ".join([t for t in v.tokens]))
        else:
            if currMS == "DG 4-7":
                for v in onPamph.verses:
                    if verseSelect in v.vno:
                        if transLevel == "Diplomatic":
                            aa.write(f"{v.vno} " + " ".join([t.diplomatic for t in v.tokens]))
                        if transLevel == "Normalized":
                            aa.write(f"{v.vno} " + " ".join([t.normalized for t in v.tokens]))
                        if transLevel == "Facsimile":
                            aa.write(f"{v.vno} " + " ".join([t.facsimile for t in v.tokens]))
                        if transLevel == "Lemmatized":
                            aa.write(f"{v.vno} " + " ".join([t.lemma for t in v.tokens]))
            else:
                for v in currTxt.verses:
                    if verseSelect in v.vno:
                        aa.write(f"{v.vno} {' '.join([t for t in v.tokens])}")


def display_para():
    graph = GraphDatabase.driver("neo4j://localhost:7687", auth=('neo4j', '12'))
    verseSelect = st.text_input("Select Verse or Verserange")
    txtWits = ['B1', 'P3', 'To', 'W1', 'DG4-7']
    txtSelect = st.multiselect(label="Select witnesses", options = txtWits, default=txtWits)
    displayMode = st.radio("Select display mode", options=[1, 2])
    if st.button("Run"):
        if "-" in verseSelect:
            i, ii = verseSelect.split("-")
            vRange = list(map(str, range(int(i), int(ii)+1)))
            vRange = [str(x) for x in vRange]
        if not verseSelect:
            st.write("You gotta give me some text to work with")
        if displayMode == 1:
            colNo = len(txtSelect)
            cols = st.columns(colNo)
            for a, aa in enumerate(cols):
                currTxt = txtSelect[a]
                aa.write(currTxt)
                if not verseSelect:
                    aa.write("No Verse or Verserange selected")
                if currTxt == 'DG4-7':
                    with graph.session() as session:
                        tx = session.begin_transaction()
                        results = tx.run(f"MATCH (a:E33_Linguistic_Object) WHERE a.paraVerse IN {vRange} AND a.inMS = '{currTxt}' RETURN a.paraVerse AS vn, a.Normalized AS text")
                        resD = results.data()
                else:
                    with graph.session() as session:
                        tx = session.begin_transaction()
                        results = tx.run(f"MATCH (a:E33_Linguistic_Object) WHERE a.VerseNorm IN {vRange} AND a.inMS = '{currTxt}' RETURN a.VerseNorm AS vn, a.Normalized AS text")
                        resD = results.data()
                resX = {}
                for res in resD:
                    if res['vn'] in resX:
                        resX[res['vn']] = f"{resX[res['vn']]} {res['text']}"
                    else:
                        resX[res['vn']] = res['text']
                for k in resX:
                    aa.write(f"{k} {resX[k]}")
        elif displayMode == 2:
            st.write("Nothing to see here yet.")
            st.write(f"Getting Verses {vRange} from MSs {txtSelect}")
            with graph.session() as session:
                tx = session.begin_transaction()
                results = tx.run(f"""MATCH (a)-[r]->(b) 
                                    WHERE a.inMS IN {txtSelect}
                                    AND b.inMS IN {txtSelect}
                                    AND a.VerseNorm IN {vRange} 
                                    RETURN *""")
                nodes = list(results.graph()._nodes.values())
                rels = list(results.graph()._relationships.values())
            graph_view = neo2st.get_view(nodes, rels)
            components.html(graph_view, height = 900, width=900, scrolling=True)
            

def vcooc():
    "This funciont will load the previously generated word coocurrence matrices."
    docList = ['B1', 'P3', 'To', 'W1']
    frameDict = {}
    for i in docList:
        frameDict[i] = pd.read_csv(f"latmat/{i}-cooc-results.csv", index_col=0)    
    pairings = st.multiselect('Select your pairing', frameDict)
    for i in pairings:
        st.write(i)
        ag(frameDict[i])


def words_of_interest() -> None:
    onpWords = ut.onp_dataset()
    ag(onpWords)


def main():
    ON, Lat = data_loader()
    currentData = myData(ON, Lat)
    choices = {"Parallel text display": para_display,
                "Lemmata of interest": words_of_interest,
                "Word cooccurences": vcooc,
                "Graph based paras": display_para}
    choice = st.sidebar.selectbox(label="Menu", options=choices.keys())
    if choice == 'Parallel text display':
        para_display(currentData)
    else:
        display = choices[choice]
        display()
    
    


# Display part
# -----------

main()