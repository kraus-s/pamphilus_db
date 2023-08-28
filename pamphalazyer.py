import streamlit as st
import pandas as pd
from utils import menota_parser
from utils import latin_parser
from utils import util as ut
from utils import neo2st
from utils.constants import *
import pickle
from pathlib import Path
from st_aggrid import AgGrid as ag
from neo4j import GraphDatabase
from neo4j.graph import Node, Relationship
import networkx as nx
import matplotlib.pyplot as plt
from typing import List, Dict
from pyvis import network as net
from IPython.core.display import display, HTML
import streamlit.components.v1 as components
import random
import string
from utils import n2vmhandler as n2v
from utils import similarities as sims
from IPython.core.display import display, HTML
import sqlite3
from streamlit_image_select import image_select


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
        latin = latin_parser.parse_pamphilus("data/latin/texts/pamphilus/pamphLat.xml")
        f = open("data/pickleLat.p", 'w+b')
        pickle.dump(latin, f)
        f.close()
    return onPamph, latin


def get_id():
    rID = ''.join([random.choice(string.ascii_letters
            + string.digits) for n in range(48)])
    return rID


# Display functions
# -----------------

def onp_n2v():
    gallery_table = st.radio(label="Display gallery of clusterings or tabel of available models", options=["Gallery", "Table"])
    all_models = n2v.load_model_metadata()
    name_resolution_dict = n2v.create_witness_lookup()
    if gallery_table == "Table":
        st.dataframe(all_models)
        model_select = st.selectbox(label="Select a model to display and query", options=all_models["File Name"].to_list())
        _show_model(all_models, model_select, name_resolution_dict)
    elif gallery_table == "Gallery":
        st.write("Hello")
        model_meta = "asdf"
        img = image_select("All available plots", n2v.get_all_plot_paths())
        if st.button("Load model"):
            model_select = n2v.get_model_from_plot_path(img)
            _show_model(all_models, model_select, name_resolution_dict)
            

def _show_model(all_models: pd.DataFrame, model_select: str, names_dict: dict[str, str]):
    selected_model_metadata = all_models.loc[all_models["File Name"] == model_select].to_dict('records')[0]
    model = n2v.load_n2v_model(model_select)
    img_path = n2v.get_plot(model_select)
    st.image(img_path)
    query_options = n2v.get_applicable_witnesses(date_range_init=selected_model_metadata['Date Range'], name_lookup=names_dict)
    query_model = st.selectbox(label="Select a witnesses to retrieve learned similarities", options=list(query_options.keys()))
    how_many = st.number_input(label="Load top n of the most similar witnesses", value=10)
    similars = n2v.get_similars(model=model, onpID=query_options[query_model], nsimilars=how_many)
    s = ''
    for i in similars:
        hs_serch_string = i[2].replace(" ", "+")
        s += f"- [{names_dict[i[0]]}](https://onp.ku.dk/onp/onp.php{i[1]}) Search for MS on [handrit.is](https://handrit.is/search?q={hs_serch_string}) \n"
    st.markdown(s)
    cluster_explanations = _get_cluster_docs(model_select, names_dict)
    for i in cluster_explanations.keys():
        with st.expander(f"Show items in cluster {i}"):
            st.markdown(cluster_explanations[i])


def _get_cluster_docs(model_filename: str, names_dict: dict[str, str]) -> dict[str, str]:
    table_filename = model_filename.replace("n2v", "xlsx")
    table_df = pd.read_excel(f"{N2V_TABLES_PATH}{table_filename}")
    cluster_groups = table_df.groupby("kmeans")
    res = {}
    for name, group in cluster_groups:
        s = ''
        for i, row in group.iterrows():
            s += f"- [{names_dict[row['hrf_names']]}](https://onp.ku.dk/onp/onp.php{row['node_ids']}) \n"
        res[name] = s
    return res


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


def data_entry_helper():
    if Path("/data/ingest/nodes.csv").is_file():
        nodesDF = pd.read_csv("/data/ingest/nodes.csv")
    else:
        nodesDF = pd.DataFrame(columns=['NodeID', 'Labels', 'Properties'])
    if Path("/data/ingest/edges.csv").is_file():
        edgeDF = pd.read_csv("/data/ingest/nodes.csv")
    else:
        edgeDF = pd.DataFrame(columns=['fromID', 'toID', 'HRF'])
    labelList = ['E22_Human-Made_Object', 'E21_Person', 'E53_Place']
    propsList22 = ['Signature', 'Abbreviation', 'Provenance (Place)']
    propsList21 = ['Name', 'Year born', 'Year died', 'GND']
    whatToDo = st.selectbox('Was eingeben?', ['Person', 'Object/Artifact', 'Edge'])
    with st.form("Data entry"):
        if whatToDo == 'Person':
            name = st.text_input('name')
            yob = st.text_input('Year of birth')
            yod = st.text_input('Year of death')
            gnd = st.text_input('GND Identifier if any')
            submitted = st.form_submit_button('Submit')
            if submitted:
                id = get_id()
                apDict = {'NodeID': id, 'Labels': 'E22_Human-Made_Object', 'Properties': f"name: '{name}', born: '{yob}', died: '{yod}', GND-ID: '{gnd}'"}
                nodesDF = nodesDF.append(apDict)
                nodesDF.to_csv("/data/ingest/nodes.csv")
        if whatToDo == 'Object/Artifact':
            sig = st.text_input('Signature')
            abb = st.text_input('Abbreviation')
            provenance = st.text_input("Where is it from?")
            submitted = st.form_submit_button()
            if submitted:
                id = get_id()
                apDict = {'NodeID': id, 'Labels': 'E22_Human-Made_Object'}


def get_all_stylo():
    all_metrics = sims.get_csv_filenames()
    selected_table = st.selectbox(label="Select a similarity type", options=all_metrics)
    df = sims.get_similarity(f"data/similarities/{selected_table}")
    st.dataframe(df)


def splitsies(combined_name: str) -> str:
    return combined_name.split("-")[1]


def get_leven_dfs_ready(df: pd.DataFrame, leven_similarity: int, leven_similarity_upper: int, simplify: bool = False):
    strings_to_check = ["B1", "P3", "W1", "To", "P5"]
    filtered_df = df[df['v1'].str.contains('|'.join(strings_to_check))]
    filtered_df["v_number_v1"] = filtered_df["v1"].apply(splitsies)
    filtered_df["v_number_v2"] = filtered_df["v2"].apply(splitsies)
    filtered_df = filtered_df[filtered_df["v_number_v1"] != filtered_df["v_number_v2"]]
    filtered_df = filtered_df[filtered_df["v1"] != ""]
    filtered_df.drop(columns=["v_number_v1", "v_number_v2", "locID"], inplace=True)
    filtered_df = filtered_df.loc[(filtered_df["score"] >= leven_similarity) & (filtered_df["score"] <= leven_similarity_upper)]
    if simplify:
        filtered_df = filtered_df[~filtered_df['v2'].str.contains('|'.join(strings_to_check))]
        filtered_df = filtered_df.groupby()
        return filtered_df
    else:
        return filtered_df


def get_leven_df() -> pd.DataFrame:
    db = sqlite3.connect(LEVEN_DB)
    df = pd.read_sql("SELECT * FROM scores", db)
    return df


def display_leven():
    df = get_leven_df()
    simplify = st.checkbox("Simplify output by removing all entries, that show Levenshtein Scores between Verses of Pamphilus; group results by verses and sort.")
    st.write("Model subset selection: Select all models, models including all ONP data, or only models excluding diplomas and legal sources")
    display_stop_docs = st.radio("Selection:", ["All models", "Models with all data", "Models exluding diplomas/legal sources"])
    leven_similarity = st.slider("Levenshtein lower threshold", min_value=50, max_value=100, value=60)
    leven_similarity_upper = st.slider("Levenshtein upper threshold", min_value=50, max_value=100, value=99)
    filtered_df = get_leven_dfs_ready(df, leven_similarity, leven_similarity_upper, simplify)
    st.dataframe(filtered_df)


def main():
    ON, Lat = data_loader()
    currentData = myData(ON, Lat)
    choices = {"Parallel text display": para_display,
                "Lemmata of interest": words_of_interest,
                "Word cooccurences": vcooc,
                "Graph based paras": display_para,
                "Node2Vec similarities": onp_n2v,
                "Stylometrics and Similarities": get_all_stylo,
                "Levenshtein similarities (Latin)": display_leven}
    choice = st.sidebar.selectbox(label="Menu", options=choices.keys())
    if choice == 'Parallel text display':
        para_display(currentData)
    else:
        display = choices[choice]
        display()
    

# Display part
# -----------

main()