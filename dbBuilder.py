from platform import node
from typing import Tuple
from neo4j import GraphDatabase
from pathlib import Path
import pandas as pd
from utils.constants import *
from utils.latin_parser import latin_neofyier as ln
from utils.menota_parser import para_neofiyer as pn
from utils import menota_parser as mp
import glob
from utils.util import read_tei
from bs4 import BeautifulSoup


# Constants
###########


safeHouse = Path('.')
latMat = Path('./data/latMat/')
norseMat = Path('./data/norseMat/')
bowlMat = Path('./data/bowlMat/')
paraMat = Path('./data/paraMat/')

remoteDB = "philhist-sven-1.philhist.unibas.ch"
localDB = "localhost"


def load_stilometrics() -> pd.DataFrame:
    '''Helper function to load the stilometric data, i.e. cosine DISTANCE of different texts'''
    df = pd.read_csv('latinLemmatized-cosine.csv', header=0, index_col=0)
    edgeDF = pd.DataFrame(columns=['FromNode', 'ToNode', 'EdgeLabels', 'EdgeProps', 'HRF'])
    for index, row in df.iterrows():
        if index in TXTLOOKUPDICT:
            indexy = TXTLOOKUPDICT[index]
        else:
            indexy = index
        for i in list(df.columns):
            if i in TXTLOOKUPDICT:
                ii = TXTLOOKUPDICT[i]
            else:
                ii = i
            if not i == index:
                edgeDF = edgeDF.append({'FromNode': f"'{indexy}'", 'ToNode': f"'{ii}'", 'EdgeLabels': f"{INTERTEXTREL}", 'EdgeProps': f"Weigth: {float(row[i])}"}, ignore_index=True)
    return edgeDF


def load_norse_constil() -> Tuple[pd.DataFrame, pd.DataFrame]:
    '''Helper function to load titles, manuscripts and stilometrics for the old norse ms context'''
    fList = glob.glob(f"{OLD_NORSE_CORPUS_FILES}*.xml")
    edgeDF = pd.DataFrame(columns=['FromNode', 'ToNode', 'EdgeLabels', 'EdgeProps', 'HRF'])
    nodeDF = pd.DataFrame(columns=['NodeID', 'NodeLabels', 'NodeProps'])
    stylo = pd.read_csv('norse-lemmatized-cosine.csv', header=0, index_col=0)
    stylo_lookup = {}
    have_ms = []
    for i in fList:
        soup = read_tei(i)
        mss, txt, origPlace = mp.getInfo(soup, get_all=True)
        if "DG-4at7" in i:
            if "Pamph" in i:
                curr = PAMPHFAMID
            if "Elis" in i:
                curr = ELISFAMID
            if "Streng" in i:
                curr = STRENGFAMID
            stylo_lookup[txt] = curr
        elif 'Alex' in txt:
            stylo_lookup[txt] = ALEXSAGA519
        else:
            if not mss in have_ms:
                nodeDF = nodeDF.append({"NodeID": f'"{mss.replace(" ", "_")}"', 'NodeLabels': f'{MSSDESC}', 'NodeProps': f"Signature: '{mss}', Origin: '{origPlace}'"}, ignore_index=True)
                if not origPlace == 'N/A':
                    edgeDF = edgeDF.append({'FromNode': f'"{mss.replace(" ", "_")}"', 'ToNode': f"'{LOCLOOK[origPlace]}'", 'EdgeLabels': f"{FROMWHERE}"}, ignore_index=True)
            nodeDF = nodeDF.append({"NodeID": f'"{i}"', 'NodeLabels': f'{TXTWITDES}', 'NodeProps': f"Title: '{txt}', InMS: '{mss}', inFam: 'N/A', famID: 'N/A', AuthorID: 'nan', Language: 'ON', Meter:'Prose'"}, ignore_index=True)
            edgeDF = edgeDF.append({'FromNode': f'"{i}"', 'ToNode': f'"{mss.replace(" ", "_")}"', 'EdgeLabels': f'InMS'}, ignore_index=True)
            
            stylo_lookup[txt] = i
        have_ms.append(mss)
    for index, row in stylo.iterrows():
        for i in list(stylo.columns):
            if not i == index:
                edgeDF = edgeDF.append({'FromNode': f"'{stylo_lookup[index]}'", 'ToNode': f"'{stylo_lookup[i]}'", 'EdgeLabels': f"{INTERTEXTREL}", 'EdgeProps': f"Weigth: {float(row[i])}"}, ignore_index=True)
    return nodeDF, edgeDF


def load_contexts() -> Tuple[pd.DataFrame, pd.DataFrame]:
    '''Helper function to ingest several Excel files with information on manuscripts etc.'''
    
    # This loads the infomartion on the core manuscripts and contained text witnesses (B1, P3, To, W1)
    wtns = pd.read_excel(f"{EXCELS}hss-txtwtns.xlsx")
    nodeDF = pd.DataFrame(columns=['NodeID', 'NodeLabels', 'NodeProps'])
    edgeDF = pd.DataFrame(columns=['FromNode', 'ToNode', 'EdgeLabels', 'EdgeProps', 'HRF'])
    for index, row in wtns.iterrows():
        nodeDF = nodeDF.append({
            'NodeID': f"'{row['TxtWitID']}'", 
            'NodeLabels': f'{TXTWITDES}', 
            'NodeProps': f"Title: '{row['Name']}', InMS: '{row['Abbreviation']}', inFam: '{row['TextFamily']}', famID: '{row['TxtFamID']}', AuthorID: '{row['AuthURI']}', Language: '{row['lang']}', Meter: '{row['Meter']}'"}, 
            ignore_index=True)
        edgeDF = edgeDF.append({
            'FromNode': f"'{row['TxtWitID']}'",
            'ToNode': f"'{row['Abbreviation']}'",
            'EdgeLabels': 'InMS'
        }, ignore_index=True)
    mss = pd.read_excel(f"{EXCELS}mss.xlsx")
    for index, row in mss.iterrows():
        nodeDF = nodeDF.append({'NodeID': f"'{row['Abbreviation']}'", 
        'NodeLabels': f"{MSSDESC}",'NodeProps': f"Signature: '{row['Signature']}', Country: '{row['Country']}'"}, ignore_index=True)
        edgeDF = edgeDF.append({'FromNode': f"'{row['Abbreviation']}'", 'ToNode': f"'{row['Country']}'", 'EdgeLabels': f"{FROMWHERE}"}, ignore_index=True)
    pers = pd.read_excel(f"{EXCELS}pers.xlsx")
    for index, row in pers.iterrows():
        nodeDF = nodeDF.append({'NodeID': f"'{row['URI']}'", 'NodeLabels': f"{PERSONLABEL}", 'NodeProps': f"Name: '{row['Name']}', URI: '{row['URI']}'"}, ignore_index=True)
    # This loads all the other texts that we have stilometric data on
    wtns = pd.read_excel(f"{EXCELS}txts-womss.xlsx")
    for index, row in wtns.iterrows():
        nodeDF = nodeDF.append({'NodeID': f"'{row['TxtFamID']}'", 'NodeLabels': f"{FAMDESC}", 'NodeProps': f"{FAMLIT}: '{row['TextFamily']}', famID: '{row['TxtFamID']}', AuthorID: '{row['AuthURI']}'"}, ignore_index=True)  
    return nodeDF, edgeDF


def data_collector() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Gets all the data needed. Should this be a factory method? Idk.
        Args:
            None
        Returns: 
            allNodes(pd.DataFrame): Dataframe of all nodes ready to be ingested
            allEdges(pd.DataFrame): Dataframe of all edges ready to be ingested"""
    interEdges = load_stilometrics()
    conNodes, conEdges = load_contexts()
    norNodes, norEdges = load_norse_constil()
    addnodes = pd.read_excel(f"{EXCELS}addnodes.xlsx")
    addedges = pd.read_excel(f"{EXCELS}addedges.xlsx")  
    # latNodes, latEdges = ln(infile=PAMPHILUS_LATINUS)
    # paraNodes, paraEdges = pn(infile=PSDG47)
    allNodes = pd.concat([conNodes, norNodes, addnodes])
    allEdges = pd.concat([conEdges, interEdges, norEdges, addedges])
    return allNodes, allEdges



def db_commit(nodeDF: pd.DataFrame, edgeDF: pd.DataFrame):
    graph = GraphDatabase.driver("neo4j://localhost:7687", auth=('neo4j', '12'))
    edgeDF = edgeDF.fillna("weight: 0")
    print("Dataframes going in")

    # This will create all nodes from the nodes list
    with graph.session() as session:
        tx = session.begin_transaction()
        for index, row in nodeDF.iterrows():
            tx.run(f"CREATE (a: {row['NodeLabels']} {{{row['NodeProps']}, nodeID: {row['NodeID']}}})")
        tx.commit()
    
    # Query to link textwitnesses to their textual families
    # Nodes for textual famlies are created in the process
    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run(
            f"""MATCH (a:{TXTWITDES})
            MERGE (b:{FAMDESC} {{{FAMLIT}: a.inFam, famID: a.famID, AuthorID: a.AuthorID, nodeID: a.famID}})
            MERGE (a)-[r:{INFAMDESC}]->(b)
            """
        )
        tx.commit()

    # This will create all edges from the edges list
    with graph.session() as session:
        tx = session.begin_transaction()
        for index, row in edgeDF.iterrows():
            tx.run(f"MATCH (a), (b) WHERE a.nodeID = {row['FromNode']} AND b.nodeID = {row['ToNode']} AND NOT a.nodeID = b.nodeID CREATE (a)-[r:{row['EdgeLabels']} {{{row['EdgeProps']}}} ]->(b)")
        tx.commit()

    print("Creating extra relationships")
    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run("""MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
                        WHERE a.Normalized = b.Normalized
                        AND a.VerseDipl = b.VerseDipl
                        AND a.pos = b.pos
                        AND NOT a.nodeID = b.nodeID
                        AND NOT a.inMS = b.inMS
                        MERGE (a)-[r:para_specific_dipl]->(b)""")
        tx.commit()

    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run("""MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
                    WHERE a.Normalized = b.Normalized
                    AND a.VerseDipl = b.VerseDipl
                    AND NOT a.nodeID = b.nodeID
                    AND NOT a.inMS = b.inMS
                    MERGE (a)-[r:para_unspecific_dipl]->(b)""")
        tx.commit()

    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run("""MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
                        WHERE a.Normalized = b.Normalized
                        AND a.VerseNorm = b.VerseNorm
                        AND a.pos = b.pos
                        AND NOT a.nodeID = b.nodeID
                        AND NOT a.inMS = b.inMS
                        MERGE (a)-[r:para_specific_norm]->(b)""")
        tx.commit()

    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run("""MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
                    WHERE a.Normalized = b.Normalized
                    AND a.VerseNorm = b.VerseNorm
                    AND NOT a.nodeID = b.nodeID
                    AND NOT a.inMS = b.inMS
                    MERGE (a)-[r:para_unspecific_norm]->(b)""")
        tx.commit()

    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run("""MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
                    WHERE a.paraMS = b.inMS
                    AND a.paraVerse = b.VerseNorm
                    CREATE (a)-[r:translation_para_norm]->(b)
                """)
        tx.commit()

    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run("""MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
                    WHERE a.paraMS = b.inMS
                    AND a.paraVerse = b.VerseDipl
                    CREATE (a)-[r:translation_para_dipl]->(b)
                """)
        tx.commit()

    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run(
            """MATCH (a:ZZ1_Verse), (b:ZZ1_Verse)
            WHERE a.VerseDipl = b.VerseDipl
            AND NOT a.inMS = b.inMS
            CREATE (a)-[r:verse_aligned_dipl]->(b)
            """
        )
        tx.commit()

    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run(
            """MATCH (a:ZZ1_Verse), (b:ZZ1_Verse)
            WHERE a.VerseNorm = b.VerseNorm
            AND NOT a.inMS = b.inMS
            CREATE (a)-[r:verse_aligned_norm]->(b)
            """
        )
        tx.commit()
    

    # Query to link textual families to their authors, if known
    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run(
            f"""MATCH (a:{FAMDESC}), (b:{PERSONLABEL})
            WHERE a.AuthorID = b.URI
            AND NOT a.AuthorID = 'nan' OR b.AuthordID = 'nan'
            CREATE (b)-[r:{AUTHREL}]->(a)
            """
        )
        tx.commit()
    
    # Match and create text language annotations
    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run(
            f"""MATCH (a:{TXTWITDES}) 
            MERGE (b:{NATLANG} {{Language: a.Language}})
            MERGE (a)-[r:{INLANG}]->(b)
            """
        )
        tx.commit()
    
    # Match and create the metric annotation
    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run(
            f""" MATCH (a:{TXTWITDES})
            MERGE (b:{METRE} {{Type: a.Meter}})
            MERGE (a)-[r:{HASMETRE}]->(b)
            """
        )
        tx.commit()
    print("Done!")
    return

def main():
    nodeDF, edgeDF = data_collector()
    db_commit(nodeDF, edgeDF)


if __name__ == "__main__":
    main()