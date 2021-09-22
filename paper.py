import pandas as pd
from dbBuilder import read_tei
from bs4 import BeautifulSoup
import lxml
from neo4j import GraphDatabase


def latParser(inFile):
    soup = read_tei(inFile)
    verses = soup.find_all('v')
    nodeDF = pd.DataFrame(columns=['NodeID', 'NodeLabels', 'NodeProps'])
    edgeDF = pd.DataFrame(columns=['FromNode', 'ToNode', 'EdgeLabels', 'EdgeProps', 'HRF'])
    prevNodeID = 0
    sourceDict = {'B1': 'Ms. Hamilton 390', 'P3': 'cod. lat. 8430 4°', 'To': 'cod. 102-11', 'W1': 'cod. 303 HAN MAG 8°', 'F': 'MS Fragm. lat II 11'}
    msDict = {'B1': 'Ms. Hamilton 390', 'P3': 'cod. lat. 8430 4°', 'To': 'cod. 102-11', 'W1': 'cod. 303 HAN MAG 8°'}
    edgeHelperDict = {}
    edgeHelperDict2 = {}
    for key, value in sourceDict.items():
        msID = value.replace(" ", "")
        updog = {key: msID}
        edgeHelperDict2.update(updog)
        nodeDF = nodeDF.append({'NodeID': f"'{msID}'",
                                'NodeLabels': 'E18_Physical_Thing',
                                'NodeProps': f"Signature: '{value}', Abbreviation: '{key}'"},
                                ignore_index=True)
    countID = 0

    for indiVerse in verses:
        
        currVerse = indiVerse.get('n')
        
        words2beWorked = indiVerse.findAll('w')
        for realtalk in words2beWorked:
            variants = realtalk.findAll('var')
            varCount = len(variants)
            if varCount == 2:
                for var in variants:
                    which_source = var.get('variants')
                    if which_source == "a":
                        for ms in msDict.keys():
                            actualWord = var.get_text()
                            currID0 = id(actualWord)
                            currID = f"{ms}-{countID}"
                            nodeDF = nodeDF.append({'NodeID': f"'{currID}'", 
                                                    'NodeLabels': 'E33_Linguistic_Object', 
                                                    'NodeProps': f"Normalized: '{actualWord}', Verse: '{currVerse}', inMS: '{ms}', pos: '{countID}'"},
                                                    ignore_index=True)
                            try:
                                prevNodeID = edgeHelperDict[ms]
                            except:
                                print(f"First run on {ms} A")
                            if prevNodeID:
                                edgeDF = edgeDF.append({'FromNode': f"'{prevNodeID}'",
                                                        'ToNode': f"'{currID}'",
                                                        'EdgeLabels': 'next',
                                                        'HRF': f"{ms} - {actualWord} to next"},
                                                        ignore_index=True)
                                edgeDF = edgeDF.append({'FromNode': f"'{currID}'",
                                                        'ToNode': f"'{edgeHelperDict2[ms]}'",
                                                        'EdgeLabels': 'inMS',
                                                        'HRF': f"{ms} - {actualWord} to MS A"},
                                                        ignore_index=True)
                            elif not prevNodeID:
                                edgeDF = edgeDF.append({'FromNode': f"'{currID}'",
                                                        'ToNode': f"'{edgeHelperDict2[ms]}'",
                                                        'EdgeLabels': 'inMS',
                                                        'HRF': f"{ms} - {actualWord} to MS B"},
                                                        ignore_index=True)
                            updog = {ms: currID}
                            edgeHelperDict.update(updog)
                    if which_source == "F":
                        ms = "F"
                        actualWord = var.get_text()
                        currID = f"{ms}-{countID}"
                        nodeDF = nodeDF.append({'NodeID': f"'{currID}'", 
                                                'NodeLabels': 'E33_Linguistic_Object', 
                                                'NodeProps': f"Normalized: '{actualWord}', Verse: '{currVerse}', inMS: '{ms}', pos: '{countID}'"},
                                                ignore_index=True)
                        try:
                            prevNodeID = edgeHelperDict["F"]
                        except:
                            print("First run on F - bitch!")
                        if prevNodeID:
                            edgeDF = edgeDF.append({'FromNode': f"'{prevNodeID}'",
                                                    'ToNode': f"'{currID}'",
                                                    'EdgeLabels': 'next',
                                                    'HRF': f"F - {actualWord} to next"},
                                                    ignore_index=True)
                            edgeDF = edgeDF.append({'FromNode': f"'{currID}'",
                                                    'ToNode': f"'{edgeHelperDict2[ms]}'",
                                                    'EdgeLabels': 'inMS',
                                                    'HRF': f"F - {actualWord} to MS C"},
                                                    ignore_index=True)
                        else:
                            edgeDF = edgeDF.append({'FromNode': f"'{currID}'",
                                                    'ToNode': f"'{edgeHelperDict2[ms]}'",
                                                    'EdgeLabels': 'inMS',
                                                    'HRF': f"F - {actualWord} to MS D"},
                                                     ignore_index=True)
                        updog = {"F": currID}
                        edgeHelperDict.update(updog)
            else:
                for var in variants:
                    variantTrain = var.get('variants')
                    multiVar = variantTrain.split(', ')
                    if len(multiVar) == 1:
                        currSource = var.get('variants')
                        ms = currSource
                        actualWord = var.get_text()
                        currID0 = id(actualWord)
                        currID = f"{ms}-{countID}"
                        nodeDF = nodeDF.append({'NodeID': f"'{currID}'", 
                                                'NodeLabels': 'E33_Linguistic_Object', 
                                                'NodeProps': f"Normalized: '{actualWord}', Verse: '{currVerse}', inMS: '{ms}', pos: '{countID}'"},
                                                ignore_index=True)
                        try:
                            prevNodeID = edgeHelperDict[currSource]
                        except:
                            print(f"First run on {currSource}")
                        if prevNodeID:
                            edgeDF = edgeDF.append({'FromNode': f"'{prevNodeID}'",
                                                    'ToNode': f"'{currID}'",
                                                    'EdgeLabels': 'next',
                                                    'HRF': f"{ms} - {actualWord} to next"},
                                                    ignore_index=True)
                            edgeDF = edgeDF.append({'FromNode': f"'{currID}'",
                                                    'ToNode': f"'{edgeHelperDict2[ms]}'",
                                                    'EdgeLabels': 'inMS',
                                                    'HRF': f"{ms} - {actualWord} to MS E"},
                                                    ignore_index=True)
                        else:
                            edgeDF = edgeDF.append({'FromNode': f"'{currID}'",
                                                    'ToNode': f"'{edgeHelperDict2[ms]}'",
                                                    'EdgeLabels': 'inMS',
                                                    'HRF': f"{ms} - {actualWord} to MS F"},
                                                    ignore_index=True)
                        updog = {currSource: currID}
                        edgeHelperDict.update(updog)
                    elif len(multiVar) >= 2:
                        for indiVar in multiVar:
                            currSource = indiVar
                            ms = currSource
                            actualWord = var.get_text()
                            currID0 = id(actualWord)
                            currID = currID = f"{currSource}-{countID}"
                            nodeDF = nodeDF.append({'NodeID': f"'{currID}'", 
                                                    'NodeLabels': 'E33_Linguistic_Object', 
                                                    'NodeProps': f"Normalized: '{actualWord}', Verse: '{currVerse}', inMS: '{ms}', pos: '{countID}'"},
                                                    ignore_index=True)
                                
                            try:
                                prevNodeID = edgeHelperDict[currSource]
                            except:
                                print(f"First run on {currSource}")
                            if prevNodeID:
                                edgeDF = edgeDF.append({'FromNode': f"'{prevNodeID}'",
                                                        'ToNode': f"'{currID}'",
                                                        'EdgeLabels': 'next',
                                                        'HRF': f"{ms} - {actualWord} to next"},
                                                        ignore_index=True)
                                edgeDF = edgeDF.append({'FromNode': f"'{currID}'",
                                                        'ToNode': f"'{edgeHelperDict2[ms]}'",
                                                        'EdgeLabels': 'inMS',
                                                        'HRF': f"{currSource} - {actualWord} to MS G"},
                                                        ignore_index=True)
                            else:
                                edgeDF = edgeDF.append({'FromNode': f"'{currID}'",
                                                        'ToNode': f"'{edgeHelperDict2[ms]}'",
                                                        'EdgeLabels': 'inMS',
                                                        'HRF': f"{currSource} - {actualWord} to MS H"},
                                                        ignore_index=True)
                            updog = {currSource: currID}
                            edgeHelperDict.update(updog)
            countID +=1 
    return edgeDF, nodeDF


def filterDFs(edgeDF: pd.DataFrame, nodeDF: pd.DataFrame):
    posList = []
    keepList = []
    for index, row in nodeDF.iterrows():
        if "Normalized: '-'" in row['NodeProps']:
            stuff = row['NodeProps']
            stuffList = stuff.split(", ")
            posList.append(stuffList[-1])
    for index, row in nodeDF.iterrows():
        stuff = row['NodeProps']
        stuffList = stuff.split(", ")
        if stuffList[-1] not in posList:
            keepList.append(row['NodeID'])
    nodeDFout = nodeDF[nodeDF.NodeID.isin(keepList)].reset_index(drop=True)
    edgeDFout1 = edgeDF[edgeDF.ToNode.isin(keepList)].reset_index(drop=True)
    edgeDFout = edgeDFout1[edgeDFout1.FromNode.isin(keepList)].reset_index(drop=True)
    return edgeDFout, nodeDFout



if __name__ == "__main__":
    Filter = True
    edgeDF, nodeDF = latParser("paper-pamph.xml")
    if Filter:
        edgeDF, nodeDF = filterDFs(edgeDF, nodeDF)
    filterDFs(edgeDF, nodeDF)
    graph = GraphDatabase.driver("neo4j://localhost:7687", auth=('neo4j', '12'))
    with graph.session() as session:
        tx = session.begin_transaction()
        for index, row in nodeDF.iterrows():
            tx.run(f"CREATE (a: {row['NodeLabels']} {{{row['NodeProps']}, nodeID: {row['NodeID']}}})")
        tx.commit()
        tx.close()
    with graph.session() as session:
        tx = session.begin_transaction()
        for index, row in edgeDF.iterrows():
            tx.run(f"MATCH (a), (b) WHERE a.nodeID = {row['FromNode']} AND b.nodeID = {row['ToNode']} AND NOT a.nodeID = b.nodeID MERGE (a)-[r:{row['EdgeLabels']}]->(b)")
        tx.commit()
        tx.close
    # with graph.session() as session:
    #     session.run("""MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
    #                 WHERE a.Normalized = b.Normalized
    #                 AND a.Verse = b.Verse
    #                 AND NOT a.nodeID = b.nodeID
    #                 AND NOT a.inMS = b.inMS
    #                 MERGE (a)-[r:para_unspecific]->(b)""")
    # with graph.session() as session:
    #     session.run("""MATCH (a:E33_Linguistic_Object), (b:E33_Linguistic_Object)
    #                 WHERE a.Normalized = b.Normalized
    #                 AND a.Verse = b.Verse
    #                 AND a.pos = b.pos
    #                 AND NOT a.nodeID = b.nodeID
    #                 AND NOT a.inMS = b.inMS
    #                 MERGE (a)-[r:para_specific]->(b)""")
    print("Done.")