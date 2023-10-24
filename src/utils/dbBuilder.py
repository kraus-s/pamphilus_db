from platform import node
from neo4j import GraphDatabase
import pandas as pd
from utils.constants import *
from utils.latin_parser import latin_neofyier as ln
from utils.menota_parser import para_neofiyer as pn
import utils.menota_parser as mp
import glob
from utils.util import read_tei



# Constants
###########

remoteDB = "philhist-sven-1.philhist.unibas.ch"
localDB = "localhost"


def load_stilometrics() -> pd.DataFrame:
    '''Helper function to load the stilometric data, i.e. cosine DISTANCE of different texts'''
    print("load_stilometrics has started")
    df = pd.read_csv('data/similarities/stylo/latinLemmatized-cosine.csv', header=0, index_col=0)
    edges: list[tuple[str, str, str, str, str]] = []
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
            if i != index:
                current_tuple = (f"'{indexy}'", f"'{ii}'", f"{INTERTEXTREL}", f"Weigth: {float(row[i])}", "-")
                edges.append(current_tuple)
    print("load_stilometrics has finished.")
    return edges


def load_norse_constil() -> tuple[list[tuple[str, str, str]], list[tuple[str, str, str, str, str]]]:
    '''Helper function to load titles, manuscripts and stilometrics for the old norse ms context
    Nodes and edges are orgnaized as tuples and appended to lists, which are then returned.
    The tuple for every node is (NodeID, NodeLabel, NodeProps)
    The tuple for every edge is (FromNode, ToNode, EdgeLabel, EdgeProps, HRF)'''
    print("load_norse_constil has started")
    xml_files = glob.glob(f"{OLD_NORSE_CORPUS_FILES}*.xml")
    nodes: list[tuple[str, str, str]] = []
    edges: list[tuple[str, str, str, str, str]] = []
    stylo = pd.read_csv('data/similarities/stylo/norse-lemmatized-cosine.csv', header=0, index_col=0)
    stylo_lookup = {}
    have_ms = []
    for i in xml_files:
        soup = read_tei(i)
        mss, txt, origin_place = mp.getInfo(soup, get_all=True)
        print(f"Processing {txt}")
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
            if mss not in have_ms:
                current_tuple = (f"'{mss}'", f"{MSSDESC}", f"Signature: '{mss}', Origin: '{origin_place}'")
                nodes.append(current_tuple)
                if origin_place != 'N/A':
                    current_tuple = (f"'{mss}'", f"'{origin_place}'", f"{FROMWHERE}", "prop: ['nan']", f"{mss} from {origin_place}")
                    edges.append(current_tuple)
            node_tuple = (f"'{txt}'", f"{TXTWITDES}", f"Title: '{txt}', InMS: '{mss}', inFam: 'N/A', famID: 'N/A', AuthorID: 'nan', Language: 'ON', Meter:'Prose'")
            nodes.append(node_tuple)
            edge_tuple = (f"'{txt}'", f"'{mss}'", "in_ms", "prop: ['nan']", f"{txt} in {mss}")
            edges.append(edge_tuple)            
            stylo_lookup[txt] = i
        have_ms.append(mss)
    for index, row in stylo.iterrows():
        for i in list(stylo.columns):
            if i != index:
                try:
                    current_tuple = (f"'{stylo_lookup[index]}'", f"'{stylo_lookup[i]}'", f"{INTERTEXTREL}", f"Weigth: {float(row[i])}", "-")
                    edges.append(current_tuple)
                except KeyError:
                    print(f"{i} not lemmatized")
    print("load_norse_constil has finished.")
    return nodes, edges


def load_contexts() -> tuple[list[tuple[str, str, str]], list[tuple[str, str, str, str, str]]]:
    '''Helper function to ingest several Excel files with information on manuscripts etc.
    Nodes and edges are orgnaized as tuples and appended to lists, which are then returned.
    The tuple for every node is (NodeID, NodeLabel, NodeProps)
    The tuple for every edge is (FromNode, ToNode, EdgeLabel, EdgeProps, HRF)'''
    print("load_contexts has started")
    # This loads the infomartion on the core manuscripts and contained text witnesses (B1, P3, To, W1, P5)
    wtns = pd.read_excel(f"{EXCELS}hss-txtwtns.xlsx")
    nodes: list[tuple[str, str, str]] = []
    edges: list[tuple[str, str, str, str, str]] = []
    for index, row in wtns.iterrows():
        current_node = (f"'{row['TxtWitID']}'", f"{TXTWITDES}", f"Title: '{row['Name']}', InMS: '{row['Abbreviation']}', inFam: '{row['TextFamily']}', famID: '{row['TxtFamID']}', AuthorID: '{row['AuthURI']}', Language: '{row['lang']}', Meter: '{row['Meter']}'")
        nodes.append(current_node)
        current_edge = (f"'{row['TxtWitID']}'", f"'{row['Abbreviation']}'", "inMS", "prop: ['nan']", f"{row['TxtWitID']} in {row['Abbreviation']}")
        edges.append(current_edge)
    mss = pd.read_excel(f"{EXCELS}mss.xlsx")
    for index, row in mss.iterrows():
        current_node = (f"'{row['Abbreviation']}'", f"{MSSDESC}", f"Signature: '{row['Signature']}', Country: '{row['Country']}'")
        nodes.append(current_node)
        current_edge = (f"'{row['Abbreviation']}'", f"'{row['Country']}'", f"{FROMWHERE}", "prop: ['nan']", f"{row['Abbreviation']} from {row['Country']}")
        edges.append(current_edge)
    pers = pd.read_excel(f"{EXCELS}pers.xlsx")
    for index, row in pers.iterrows():
        current_node = (f"'{row['URI']}'", f"{PERSONLABEL}", f"Name: '{row['Name']}', URI: '{row['URI']}'")
        nodes.append(current_node)
    # This loads all the other texts that we have stilometric data on
    wtns = pd.read_excel(f"{EXCELS}txts-womss.xlsx")
    for index, row in wtns.iterrows():
        current_node = (f"'{row['TxtFamID']}'", f"{FAMLIT}", f"Family: '{row['TextFamily']}', famID: '{row['TxtFamID']}', AuthorID: '{row['AuthURI']}'")
        nodes.append(current_node)
    print("load_contexts has finished.")
    return nodes, edges


def data_collector() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Gets all the data needed. Should this be a factory method? Idk.
        Args:
            None
        Returns: 
            all_nodes(pd.DataFrame): Dataframe of all nodes ready to be ingested
            all_edges(pd.DataFrame): Dataframe of all edges ready to be ingested"""
    stylo_edges = load_stilometrics()
    context_nodes, context_edges = load_contexts()
    norse_nodes, norse_edges = load_norse_constil()
    addnodes = pd.read_excel(f"{EXCELS}addnodes.xlsx")
    addedges = pd.read_excel(f"{EXCELS}addedges.xlsx")  
    latin_nodes, latin_edges = ln(infile=PAMPHILUS_LATINUS)
    parallel_nodes, parallel_edges = pn(input_file=PSDG47)
    all_node_tuples = latin_nodes + parallel_nodes + norse_nodes + context_nodes
    all_edge_tuples = latin_edges + parallel_edges + norse_edges + stylo_edges + context_edges
    node_df_from_tuples = pd.DataFrame(all_node_tuples, columns=["NodeID", "NodeLabels", "NodeProps"])
    edge_df_from_tuples = pd.DataFrame(all_edge_tuples, columns=["FromNode", "ToNode", "EdgeLabels", "EdgeProps", "HRF"])
    all_nodes = pd.concat([node_df_from_tuples, addnodes])
    all_edges = pd.concat([edge_df_from_tuples, addedges])
    return all_nodes, all_edges


def db_commit(node_df: pd.DataFrame, edge_df: pd.DataFrame):
    graph = GraphDatabase.driver("neo4j://localhost:7687", auth=('neo4j', '12'))
    edge_df = edge_df.fillna("weight: 0")
    print("Dataframes going in")

    # This will create all nodes from the nodes list
    print("Creating nodes")
    with graph.session() as session:
        tx = session.begin_transaction()
        for index, row in node_df.iterrows():
            tx.run(f"CREATE (a: {row['NodeLabels']} {{{row['NodeProps']}, nodeID: {row['NodeID']}}})")
        tx.commit()
    
    # Query to link textwitnesses to their textual families
    # Nodes for textual famlies are created in the process
    print("Creating text family relationships")
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
    print("Creating edges")
    with graph.session() as session:
        tx = session.begin_transaction()
        for index, row in edge_df.iterrows():
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

    print("Creating parallelized relationships")
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


def main():
    nodeDF, edge_df = data_collector()
    db_commit(nodeDF, edge_df)


if __name__ == "__main__":
    main()