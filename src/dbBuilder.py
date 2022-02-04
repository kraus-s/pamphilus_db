from os import read
from typing import Tuple
from neo4j import GraphDatabase
from pathlib import Path
import pandas as pd
from utils.latin_parser import latin_neofyier as ln
from utils.menota_parser import para_neofiyer as pn

# Constants
###########


safeHouse = Path('.')
latMat = Path('./data/latMat/')
norseMat = Path('./data/norseMat/')
bowlMat = Path('./data/bowlMat/')
paraMat = Path('./data/paraMat/')

remoteDB = "philhist-sven-1.philhist.unibas.ch"
localDB = "localhost"

def data_collector() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Gets all the data needed. Should this be a factory method? Idk.
        Args:
            None
        Returns: 
            allNodes(pd.DataFrame): Dataframe of all nodes ready to be ingested
            allEdges(pd.DataFrame): Dataframe of all edges ready to be ingested"""
    latNodes, latEdges = ln(infile=f"{latMat}/pamphLat.xml")
    paraNodes, paraEdges = pn(infile=f"{paraMat}/DG-4at7-Pamph-para.xml")
    allNodes = pd.concat([latNodes, paraNodes])
    allEdges = pd.concat([latEdges, paraEdges])
    return allNodes, allEdges



def db_commit(nodeDF: pd.DataFrame, edgeDF: pd.DataFrame):
    graph = GraphDatabase.driver("neo4j://localhost:7687", auth=('neo4j', '12'))
    print("Dataframes going in")
    with graph.session() as session:
        tx = session.begin_transaction()
        for index, row in nodeDF.iterrows():
            tx.run(f"CREATE (a: {row['NodeLabels']} {{{row['NodeProps']}, nodeID: {row['NodeID']}}})")
        tx.commit()
        tx.close()
    with graph.session() as session:
        tx = session.begin_transaction()
        for index, row in edgeDF.iterrows():
            tx.run(f"MATCH (a), (b) WHERE a.nodeID = {row['FromNode']} AND b.nodeID = {row['ToNode']} AND NOT a.nodeID = b.nodeID CREATE (a)-[r:{row['EdgeLabels']}]->(b)")
        tx.commit()
        tx.close
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
        tx.close
    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run("""MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
                    WHERE a.Normalized = b.Normalized
                    AND a.VerseDipl = b.VerseDipl
                    AND NOT a.nodeID = b.nodeID
                    AND NOT a.inMS = b.inMS
                    MERGE (a)-[r:para_unspecific_dipl]->(b)""")
        tx.commit()
        tx.close
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
        tx.close
    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run("""MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
                    WHERE a.Normalized = b.Normalized
                    AND a.VerseNorm = b.VerseNorm
                    AND NOT a.nodeID = b.nodeID
                    AND NOT a.inMS = b.inMS
                    MERGE (a)-[r:para_unspecific_norm]->(b)""")
        tx.commit()
        tx.close
    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run("""MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
                    WHERE a.paraMS = b.inMS
                    AND a.paraVerse = b.VerseNorm
                    CREATE (a)-[r:translation_para_norm]->(b)
                """)
        tx.commit()
        tx.close
    with graph.session() as session:
        tx = session.begin_transaction()
        tx.run("""MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
                    WHERE a.paraMS = b.inMS
                    AND a.paraVerse = b.VerseDipl
                    CREATE (a)-[r:translation_para_dipl]->(b)
                """)
        tx.commit()
        tx.close
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
        tx.close
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
        tx.close
    print("Done!")
    return


def main():
    nodeDF, edgeDF = data_collector()
    db_commit(nodeDF, edgeDF)


if __name__ == "__main__":
    main()