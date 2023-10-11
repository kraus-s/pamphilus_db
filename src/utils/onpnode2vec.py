import sqlite3
import pandas as pd
from pyparsing import srange
from node2vec import Node2Vec
import networkx as nx
from collections import Counter
import itertools
from constants import *
from typing import Generator
from datetime import datetime
import os
import glob
import csv

# Source: Keras article
# Additional source: Stellargraph docs

def create_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    return conn


def get_on_stopwords(stops_path: str) -> list[str]:
    with open(stops_path, 'r', encoding="UTF-8-sig") as f:
        stops = f.read()
    stops = stops.split(',')
    stops = [x.strip() for x in stops]
    return stops


def get_stop_onp(stops: list[str], conn: sqlite3.Connection) -> tuple[str]:
    sqstops = tuple(stops)
    df = pd.read_sql(f"SELECT * FROM lemmata WHERE lemma IN {sqstops}", conn)
    res = tuple(df["onpID"].to_list())
    return res


def _make_network(we: dict, weights: bool = True) -> nx.Graph:
    if weights:
        edges = [(x, y, {'weight': z}) for ((x, y), z) in we.items()]
    if not weights:
        edges = [(x, y) for x, y in we.items()]
    graph = nx.Graph()
    graph.add_edges_from(edges)
    return graph


def n2v_fast(we: dict, weights: bool = True, w_length: int = 100, context_size: int = 10, return_param: int = 1, in_out_param: int = 1):
    graph = _make_network(we, weights)
    n2v = Node2Vec(graph, workers=10, walk_length=w_length, num_walks=context_size, p=return_param, q=in_out_param)
    n2m = n2v.fit(window=10)
    return n2m


def get_data(stops: list[str], conn: sqlite3.Connection, stop_docs: str, postquem: int = 1, antequem: int = 1325) -> list[list[str]]:
    """ Will retrieve data from the underlying sqlite database. It first selects all witness IDs from the
    table junctionMsxWitreal in the date range defined by the post and ante quem parameters. These witness IDs
    are then passed to the next query, which selects the found witnesses and all their associated lemmata from
    the junctionLemxWit table, filtered by the stop word list passed as the stops parameter. The result is grouped by 
    lemmata, resulting in a list of witnesses for each lemma. The same is done for the text works and manuscripts, resulting
    in lists of witnesses, that share a feature, i.e. lemma, text work, manuscript.
    """
    df4 = pd.read_sql(f"SELECT a.witID, b.onpID FROM junctionMsxWitreal AS a, msInfo as b WHERE b.postquem >= {postquem} AND b.antequem <= {antequem} AND a.msID = b.onpID", conn)
    witlist = df4["witID"].to_list()
    if stop_docs == "y":
        exclude_df = pd.read_sql(f"SELECT witID from junctionWorkxWit WHERE workID IN {tuple(EXCLUDE_DIPLOMAS)}", conn)
        stop_doc_list = exclude_df["witID"].to_list()
        witlist = [x for x in witlist if x not in stop_doc_list]
    df = pd.read_sql(f"SELECT lemID, witID FROM junctionLemxWit WHERE lemID NOT IN {stops} AND witID IN {tuple(witlist)}", conn)
    sel_list = tuple(list(set(df["witID"].to_list())))
    df = df.groupby(["lemID"])["witID"].apply(list).reset_index()
    df1 = pd.read_sql(f"SELECT workID, witID FROM junctionWorkxWit WHERE witID IN {sel_list}", conn)
    df1 = df1.drop_duplicates().reset_index(drop=True)
    df1 = df1.groupby(["workID"])["witID"].apply(list).reset_index()
    df2 = pd.read_sql(f"SELECT msID, witID FROM junctionMSxWitreal WHERE witID IN {sel_list}", conn)
    df2 = df2.groupby(["msID"])["witID"].apply(list).reset_index()
    res = df["witID"].to_list() + df1["witID"].to_list() + df2["witID"].to_list()
    return res


def gen_edgelist(data_list: list[list[str]]) -> Generator[tuple[str, str], None, None]:
    for i in data_list:
        print(len(i))
        if len(i) > 5:
            print(i)
            for c in itertools.combinations(i, 2):
                if c[0] != c[1]:
                    yield c


def get_weights(edge_gen: Generator[tuple[str, str], None, None]) -> dict:
    weight_dict = Counter(edge_gen)
    return weight_dict


def make_model(weight_dict: dict, w_length: int = 100, context_size: int = 10, return_param: int = 1, in_out_param: int = 1):
    return n2v_fast(we=weight_dict, w_length=w_length, context_size=context_size, return_param=return_param, in_out_param=in_out_param)

    
def save_model(model, date_range: tuple[int, int], w_length: int, context_size: int , return_param: int , in_out_param: int, itcnt: int, stop_docs: str) -> None:
    date = datetime.now().strftime('%Y%m%d')
    date_ranger = f"{date_range[0]}-{date_range[1]}"
    file_name = f"model-{date_ranger}-{date}-{itcnt}.n2v"
    model.wv.save(f"data/models/{file_name}")
    with open("data/model-parameters.csv", "a") as f:
        writer = csv.writer(f)
        writer.writerow([date_range, w_length, context_size, return_param, in_out_param, stop_docs, file_name])


def get_files(path: str = model_path) -> list[str]:
    return [os.path.abspath(x) for x in glob.iglob(path, recursive=True)]


def main(date_range: tuple[int, int], stop_docs: str, test_run: bool = False):
    print(f"Starting with {date_range}")
    postQuem, anteQuem = date_range
    conn = create_connection(ONP_DATABASE_PATH)
    stps = get_on_stopwords(STOPWORD_PATH)
    stops = get_stop_onp(stps, conn)
    data_list = get_data(stops, conn, postquem=postQuem, antequem=anteQuem, stop_docs=stop_docs)
    print("Got ONP data, making edge list")
    edgg = gen_edgelist(data_list)
    print("Making weights")
    weights = get_weights(edgg)
    print("Got edge list and weights, modelling.")
    itcnt = 0
    if not test_run:
        for return_param in [0.5, 1, 1.5]:
            for in_out_param in [0.5, 1, 1.5]:
                for walk_length in [50, 100, 150]:
                    for context in [5, 10, 20]:
                        model = make_model(weight_dict=weights, w_length=walk_length, context_size=context, return_param=return_param, in_out_param=in_out_param)
                        save_model(model, date_range, w_length=walk_length, context_size=context, return_param=return_param, in_out_param=in_out_param, itcnt=itcnt, stop_docs=stop_docs)
                        itcnt += 1
                        print(f"Completed one loop {itcnt}")
    if test_run:
        model = make_model(weight_dict=weights, w_length=30, context_size=10, return_param=1, in_out_param=1)
        save_model(model, date_range, w_length=30, context_size=10, return_param=1, in_out_param=1, itcnt=9999, stop_docs=stop_docs)


def flow_control():
    append_to_csv = input("Keep writing to same csv? y/n")
    if append_to_csv == "n":
        with open("data/model-parameters.csv", "w+") as f:
            writer = csv.writer(f)
            writer.writerow(["Date Range", "Walk Length", "Context Window Size", "Return Parameter", "In-Out Parameter", "used_stop_docs", "File Name"])
    stop_docs = input("Exclude Diplomas etc? y/n ")
    for date_range in DATE_RANGES:
        main(date_range, test_run=False, stop_docs=stop_docs)


if __name__ == "__main__":
    flow_control()