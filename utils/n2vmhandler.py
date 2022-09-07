from bs4 import BeautifulSoup
from gensim.models import KeyedVectors
from utils.constants import *
import requests
import sqlite3


def create_connection(db_file: str = SQLITE_PATH) -> sqlite3.Connection:
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """

    conn = sqlite3.connect(db_file)

    return conn


def get_onp_page_data(onpID: str):
    page = requests.get(f"{BASE_URL}{onpID}")
    return page


def load_n2v_model(fname: str = N2VMODEL_PATH) -> KeyedVectors:
    n2kv = KeyedVectors.load(fname)
    return n2kv


def get_similars(model: KeyedVectors, conn: sqlite3.Connection, onpID: str, nsimilars: int) -> list[tuple[str, str, str, str]]:
    shit = model.most_similar(onpID, topn=nsimilars)
    res = []
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
    test = get_similars(model=load_n2v_model(), conn=create_connection(), onpID="?r9559", nsimilars=10)
    print(test)