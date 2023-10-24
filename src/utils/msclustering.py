import itertools
from typing import Dict, Generator, List, Tuple
from collections import Counter
import pandas as pd
import sqlite3
from rapidfuzz import fuzz
import csv

from utils.constants import ONP_DATABASE_PATH, EXCLUDE_LEGAL
from utils.onp_res_dict import res_dct


def create_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    return conn


def list_cleaner(in_list: List[str]) -> Dict[str, str]:
    res = {}
    for i in in_list:
        i = str(i).replace(".", "").replace("alfa", "alpha")
        ii = i.split()
        iii = " ".join([res_dct.get(x, x) for x in ii])
        res[iii] = i
    return res


def fuzz_match(comparison: List[Tuple[str, str]]) -> Generator[Tuple[int, str, str], None, None]:
    for i in comparison:
        ratio = fuzz.token_sort_ratio(i[0], i[1])
        yield ratio, i[0], i[1]


def create_title_lookup_dict(titles):
    res: Dict[str, str] = {}
    for i in fuzz_match(titles):
        ratio, k, v = i
        if ratio > 82:
            res[k] = v
    res['Ólafs saga hins helga'] = 'Ólafs saga helga'
    return res


def gen_edgelist(data_list: List[List[str]]) -> Generator[Tuple[str, str], None, None]:
    for i in data_list:
        for c in itertools.combinations(i, 2):
            if c[0] != c[1]:
                yield c


def main():
    unittlDF = pd.read_csv("ingest/handrit_msitems.csv", names=['Shelfmark', 'Title'])

    shlfmrks = unittlDF['Shelfmark'].unique().tolist()
    ttls = unittlDF['Title'].unique().tolist()

    conn = create_connection(DATABASE_PATH)
    conn_toole = create_connection("ingest/data.db")
    curs = conn.cursor()

    curs.execute("SELECT shelfmark FROM msInfo WHERE antequem <= 1350")
    rows = curs.fetchall()
    curs.close()
    shflmrks_onp_messy = [x[0] for x in rows]

    toole_df = pd.read_sql("SELECT shelfmark, terminus_post_quem, terminus_ante_quem FROM manuscripts WHERE terminus_ante_quem BETWEEN 1000 AND 1350", conn_toole)
    conc_df_toole_hrmss = unittlDF[unittlDF['Shelfmark'].isin(toole_df['shelfmark'])].reset_index(drop=True)

    exst_shlfmrks = conc_df_toole_hrmss['Shelfmark'].to_list()
    toole_mss = list_cleaner(exst_shlfmrks)
    onp_clean = list_cleaner(shflmrks_onp_messy)
    get_toole_mss = [x for x in toole_mss.keys() if x not in onp_clean.keys()]
    handrit_lookup_list = [toole_mss[x] for x in get_toole_mss]
    lookup_res = conc_df_toole_hrmss[conc_df_toole_hrmss['Shelfmark'].isin(handrit_lookup_list)]

    postquem = 1000
    antequem = 1350
    df4 = pd.read_sql(f"SELECT a.witID, b.onpID FROM junctionMsxWitreal AS a, msInfo as b WHERE b.postquem >= {postquem} AND b.antequem <= {antequem} AND a.msID = b.onpID", conn)
    sel_list = tuple(set(df4["witID"].to_list()))
    df1 = pd.read_sql(f"SELECT workID, witID FROM junctionWorkxWit WHERE witID IN {sel_list}", conn)
    df1 = df1.drop_duplicates().reset_index(drop=True)
    df1 = df1.groupby(["workID"])["witID"].apply(list).reset_index()
    wit_id_lists = df1["witID"].to_list()
    work_list = df1["workID"].to_list()
    exclude_list = ["?v95", '?v91', '?v261', '?v375']
    df1 = df1[~df1['workID'].isin(exclude_list)]
    wit_work_rel = df1.set_index('workID')['witID'].to_dict()

    onp_id_workrel_dct = {}
    for k, v in wit_work_rel.items():
        curs = conn.cursor()
        if len(v) > 1:
            get_this = tuple(v)
            curs.execute(f"SELECT msID FROM junctionMSxWitreal WHERE witID in {get_this}")
            res = [x[0] for x in curs.fetchall()]
        else:
            curs.execute(f"SELECT msID FROM junctionMSxWitreal WHERE witID='{v[0]}'")
            res = [x[0] for x in curs.fetchall()]
        ret = []
        for i in res:
            if i not in ret:
                ret.append(i)
        onp_id_workrel_dct[k] = ret
        curs.close()

    named_work_list = []
    shelfmarks_onp_list = []
    for k, v in onp_id_workrel_dct.items():
        curs = conn.cursor()
        curs.execute(f"SELECT name FROM works WHERE onpID='{k}'")
        work_name = curs.fetchall()[0][0]
        work_name = "".join((x for x in work_name if not x.isdigit()))
        if len(v) > 1:
            get_this = tuple(v)
            curs.execute(f"SELECT shelfmark FROM msInfo WHERE onpID in {get_this}")
            shelfmarks = [x[0] for x in curs.fetchall()]
        else:
            curs.execute(f"SELECT shelfmark FROM msInfo WHERE onpID='{v[0]}'")
            shelfmarks = [x[0] for x in curs.fetchall()]
        ret = []
        for i in shelfmarks:
            if i not in ret:
                ret.append(i)
        ret_list = []
        for i in ret:
            i = i.replace('\xa0', '')
            i = i.strip()
            ret_list.append(i)
        if all(x not in work_name for x in EXCLUDE_LEGAL):
            named_work_list.append(work_name)
            shelfmarks_onp_list.append(ret_list)
        curs.close()

    handrit_titles = lookup_res['Title'].to_list()
    all_combinations = itertools.product(handrit_titles, named_work_list)

    correct_length_df = pd.DataFrame(data={'Title': named_work_list, 'Shelfmark': shelfmarks_onp_list})
    correct_length_df = correct_length_df.explode('Shelfmark')
    all_work_list_onp = correct_length_df['Title'].to_list()
    all_shelfmarks_onp = correct_length_df['Shelfmark'].to_list()

    title_lookup_dict = create_title_lookup_dict(all_combinations)
    cleaned_titles = [title_lookup_dict.get(x, x) for x in handrit_titles]
    all_titles = all_work_list_onp + cleaned_titles
    dirty_to_clean_dict_handrit = {v: k for k, v in toole_mss.items()}
    dirty_to_clean_dict_onp = {v: k for k, v in onp_clean.items()}
    clean_onp_2 = [dirty_to_clean_dict_onp.get(x, x) for x in all_shelfmarks_onp]
    clean_handrit_shelfmarks_2 = [dirty_to_clean_dict_handrit[x] for x in lookup_res['Shelfmark'].to_list()]
    all_shelfmarks = clean_onp_2 + clean_handrit_shelfmarks_2
    final_dict = {}
    final_dict['Title'] = all_titles
    final_dict['Shelfmark'] = all_shelfmarks
    final_df = pd.DataFrame(final_dict)

    # Kill kringla
    kill_kringla_df = final_df[final_df['Title'] == 'Heimskringla']
    replace_kringla_sigla = kill_kringla_df['Shelfmark'].to_list()
    handrit_instead_df = unittlDF[unittlDF['Shelfmark'].isin(replace_kringla_sigla)]
    unclean_new_titles_list = handrit_instead_df['Title'].to_list()
    clean_new_titles_list = [title_lookup_dict.get(x, x) for x in unclean_new_titles_list]
    cleaned_dict = {"Shelfmark": handrit_instead_df['Shelfmark'].to_list(), "Title": clean_new_titles_list}
    cleaned_handrit_df = pd.DataFrame(cleaned_dict)
    final_df = final_df[~final_df['Shelfmark'].isin(handrit_instead_df['Shelfmark'].to_list())]
    final_final_df = pd.concat([final_df, cleaned_handrit_df], ignore_index=True)

    # Make it so
    ffdf = final_final_df.groupby('Title')['Shelfmark'].apply(list).reset_index()
    export_df = final_final_df.groupby('Shelfmark')['Title'].apply(list).reset_index()
    edge_list = gen_edgelist(ffdf['Shelfmark'].to_list())
    ffdf.to_excel("works-mss.xlsx")

    nodes = []
    with open("edge_list.csv", 'w') as f:
        writer = csv.writer(f)
        for edge in edge_list:
            writer.writerow(edge)
            if edge[0] not in nodes:
                nodes.append(edge[0])
            if edge[1] not in nodes:
                nodes.append(edge[1])
    export_df = export_df[export_df['Shelfmark'].isin(nodes)]
    export_df.to_excel("manuscripts-and-titles.xlsx")

    txts_list_list = export_df['Title'].to_list()
    txts_len_list = [len(i) for i in txts_list_list]
    text_per_ms = Counter(txts_len_list)


if __name__ == '__main__':
    main()