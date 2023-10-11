from bs4 import BeautifulSoup
from gensim.models import KeyedVectors
from utils.constants import *
import requests
import sqlite3
import pandas as pd
import ast
import re


def create_connection(db_file: str = ONP_DATABASE_PATH) -> sqlite3.Connection:
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


def get_all_plot_paths(date_range: tuple[int, int], stop_docs: str) -> pd.DataFrame:
    models_df = load_model_metadata()
    models_df['Date Range'] = models_df['Date Range'].str.replace("(", "").str.replace(")", "").str.replace("'", "").str.split(",").apply(lambda x: (int(x[0]), int(x[1])))
    date_range_mask = models_df['Date Range'] == date_range
    used_stop_docs_mask = models_df['used_stop_docs'] == stop_docs
    filtered_df = models_df[used_stop_docs_mask & date_range_mask]
    df = pd.read_csv(N2V_PLOT_PARAMETERS_PATH)
    df = df[df['Model Filename'].isin(filtered_df['File Name'])]
    all_plots = df["Plot Filename"].to_list()
    file_names = [f"{N2V_PLOTS_BASE_PATH}{x}" for x in all_plots]
    label_list = []
    k_list = []
    for index, row in df.iterrows():
        parts = row["Plot Filename"].split("-")
        k_means = row["K"]
        label_list.append(f"From {parts[2]} to {parts[3]}; No. of clusters: {k_means}")
        k_list.append(k_means)
    res = pd.DataFrame(data={"Path": file_names, "Label": label_list, "K_Val": k_list})
    return res.sort_values(by="K_Val", ascending=False)


def get_plot(model_filename: str) -> str:
    # Pass the filename of a model, the path to the corresponding plots will be returned
    df = pd.read_csv(N2V_PLOT_PARAMETERS_PATH)
    relevant_df = df.loc[df["Model Filename"] == model_filename]
    plot_name = relevant_df["Plot Filename"].to_list()
    return f"{N2V_PLOTS_BASE_PATH}{plot_name[0]}"


def get_model_from_plot_path(plot_path: str) -> str:
    plot_fname = plot_path.split("/")[-1]
    plot_name = plot_fname.split(".")[0]
    model_name = plot_name.replace("kmeans-", "")
    return f"{model_name}.n2v"


def create_witness_lookup():
    conn = create_connection()
    cur = conn.cursor()

    query = '''
        SELECT
            w.name AS work_name,
            wt.name AS witness_name,
            mw.shelfmark AS manuscript_shelfmark
        FROM
            junctionWorkxWit AS jww
        JOIN
            works AS w ON jww.workID = w.onpID
        JOIN
            witnesses AS wt ON jww.witID = wt.onpID
        LEFT JOIN
            junctionMsxWitreal AS jmw ON wt.onpID = jmw.witID
        LEFT JOIN
            msInfo AS mw ON jmw.msID = mw.onpID;
    '''
    
    cur.execute(query)
    lookup_dict = {}
    for row in cur.fetchall():
        work_name_raw = row[0]
        work_name = re.sub(r'\d', '', work_name_raw)
        manuscript_shelfmark = row[2]
        witness_name = row[1]
        combined_string = f"{work_name}-{manuscript_shelfmark}"
        lookup_dict[witness_name] = combined_string

    conn.close()
    
    return lookup_dict
    


def get_applicable_witnesses(date_range_init: str, name_lookup: dict[str, str]) -> dict[str, str]:
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
    res = {name_lookup[row[0]]:row[1] for row in rows}
    return res


def load_n2v_model(fname: str) -> KeyedVectors:
    n2kv = KeyedVectors.load(f"{N2V_MODELS_PATH}{fname}")
    return n2kv


def get_similars(model: KeyedVectors, onpID: str, nsimilars: int = 10) -> list[tuple[str, str, str, str]]:
    """Returns the n most similar hits from the selected model. Will return a tuple FILL IN DOC"""
    try:
        hits = model.most_similar(onpID, topn=nsimilars)
    except KeyError:
        return "Not found" # I know, this is ugly...
    res = []
    conn = create_connection()
    for i in hits:
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
    print("Nothing to see here")