import dis
from turtle import color
from neo4j.graph import Node, Relationship
import networkx as nx
import matplotlib.pyplot as plt
from typing import List, Dict
from pyvis import network as net
from IPython.core.display import display, HTML


def draw_graph(nodes: List[Node], edges: List[Relationship]):
    """Takes care of ingesting neo4j data, handling it with networkx and return a drawn graph object"""
    G = nx.MultiDiGraph()
    coloringBook = []
    colorChoice = {'B1': 'blue', 'W1': 'green', 'P3': 'red', 'To': 'yellow', 'DG4-7': 'lightblue'}
    for node in nodes:
        cleanLabels = str(node._labels).replace("frozenset({'", "").replace("'})", "")
        if cleanLabels == "E33_Linguistic_Object":
            coloringBook.append(node._properties['inMS'])
            displayLabel = node._properties['Normalized']
            displayColor = colorChoice[node._properties['inMS']]
        elif cleanLabels == "E22_Human_Made_Object":
            coloringBook.append(node._properties['Abbreviation'])
            displayLabel = node._properties['Abbreviation']
            displayColor = colorChoice[node._properties['Abbreviation']]
        elif cleanLabels == 'ZZ1_Verse':
            coloringBook.append(node._properties['inMS'])
            if node._properties['inMS'] in ['B1', 'P3', 'To', 'W1']:
                displayLabel = node._properties['VerseNorm']
            else:
                displayLabel = node._properties['vno']
            displayColor = colorChoice[node._properties['inMS']]
        G.add_node(node.id, label=displayLabel, color=displayColor, labels=cleanLabels, properties=node._properties)
    for rel in edges:
        G.add_edge(rel.start_node.id, rel.end_node.id, key=rel.id, type=rel.type, properties=rel._properties)
    fig, ax = plt.subplots()
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True)
    return G
    

def get_view(nodes: List[Node], rels: List[Relationship]):
    G = draw_graph(nodes, rels)
    g4 = net.Network(height='400px', width='50%', notebook=True, heading='Results')
    g4.from_nx(G)
    g4.show_buttons(filter_=['physics', 'layout'])
    g4.show('current.html')
    HtmlFile = open("current.html", 'r')
    graph_display = HtmlFile.read() 
    return graph_display