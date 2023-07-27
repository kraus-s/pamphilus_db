from bs4 import BeautifulSoup
from gensim.models import KeyedVectors
from utils.constants import *
import requests
import sqlite3
import pandas as pd
import ast

def create_connection(db_file: str = SQLITE_PATH) -> sqlite3.Connection:
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """

    conn = sqlite3.connect(db_file)

    return conn


def load_model_metadata() -> pd.DataFrame:
    # This will load the paths for all the models and all their parameters
    df = pd.read_csv(N2V_PARAMETER_PATH)
    return df


def get_plot(model_filename: str) -> list[str]:
    # Pass the filename of a model, the path to the corresponding plots will be returned
    df = pd.read_csv(N2V_PLOT_PARAMETERS_PATH)
    relevant_df = df.loc[df["Model Filename"] == model_filename]
    names_list = relevant_df["Plot Filename"].to_list()
    res = [f"{N2V_PLOTS_BASE_PATH}{x}" for x in names_list]
    return res


def get_applicable_witnesses(date_range_init: str) -> dict[str, str]:
    date_range = ast.literal_eval(date_range_init)
    conn = create_connection()
    curs = conn.cursor()
    query = f'''
                        SELECT w.name, w.onpID
                        FROM msInfo AS m
                        JOIN junctionMsxWitreal AS j ON m.onpID = j.msID
                        JOIN witnesses AS w ON j.witID = w.onpID
                        WHERE m.postquem > {date_range[0]} AND m.antequem < {date_range[1]}
                    '''
    curs.execute(query)
    rows = curs.fetchall()
    res = {row[0]:row[1] for row in rows}
    return res


def load_n2v_model(fname: str) -> KeyedVectors:
    n2kv = KeyedVectors.load(f"{N2V_MODELS_PATH}{fname}")
    return n2kv


def get_similars(model: KeyedVectors, onpID: str, nsimilars: int) -> list[tuple[str, str, str, str]]:
    shit = model.most_similar(onpID, topn=nsimilars)
    res = []
    conn = create_connection()
    for i in shit:
        curse = conn.cursor()
        curse.execute(f"SELECT name FROM witnesses WHERE onpID = '{i[0]}'")
        name = curse.fetchall()
        curse.execute(f"SELECT a.shelfmark, a.postquem, a.antequem from msInfo as a, junctionMsxWitreal as b WHERE b.witID = '{i[0]}' AND b.msID = a.onpID")
        bla = curse.fetchall()
        if bla:
            ms = bla[0][0]
            date = f"{bla[0][1]}-{bla[0][2]}"
        else:
            ms = "???"
            date = "???"
        res.append((name[0][0], i[0], ms, date))
    return res

if __name__ == "__main__":
    pass