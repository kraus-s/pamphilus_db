import sqlite3
from typing import List
import requests
from bs4 import BeautifulSoup, SoupStrainer
from utils.constants import *
import pandas as pd
from typing import List
import time

goodBits = SoupStrainer(id="myList")
wordStrainer = SoupStrainer(class_="list-group mb-3 onp-citlist onp-noborder")
anotherListStrainer = SoupStrainer(class_="list-group")


def create_connection(db_file: str = ONP_DATABASE_PATH) -> sqlite3.Connection:
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """

    conn = sqlite3.connect(db_file)

    return conn


def db_setup(conn: sqlite3.Connection):
    curse = conn.cursor()
    curse.execute('''CREATE TABLE IF NOT EXISTS lemmata(onpID PRIMARY KEY, lemma)''')
    curse.execute("""CREATE TABLE IF NOT EXISTS citations(onpID PRIMARY KEY, citation)""")
    curse.execute("""CREATE TABLE IF NOT EXISTS manuscripts(onpID PRIMARY KEY, shelfmark)""")
    curse.execute("""CREATE TABLE IF NOT EXISTS works(onpID PRIMARY KEY, name, abbreviation)""")
    curse.execute("""CREATE TABLE IF NOT EXISTS witnesses(onpID PRIMARY KEY, name)""")
    curse.execute('''CREATE TABLE IF NOT EXISTS junctionLemxCit(locID integer PRIMARY KEY DEFAULT 0 NOT NULL, lemID, citID, FOREIGN KEY(lemID) REFERENCES lemmata(onpID), FOREIGN KEY(citID) REFERENCES citations(onpID))''')
    curse.execute('''CREATE TABLE IF NOT EXISTS junctionMsxCit(locID integer PRIMARY KEY DEFAULT 0 NOT NULL, msID, citID, FOREIGN KEY(msID) REFERENCES manuscripts(onpID), FOREIGN KEY(citID) REFERENCES citations(onpID))''')
    curse.execute('''CREATE TABLE IF NOT EXISTS junctionWorkxCit(locID integer PRIMARY KEY DEFAULT 0 NOT NULL, workID, citID, FOREIGN KEY(workID) REFERENCES works(onpID), FOREIGN KEY(citID) REFERENCES citations(onpID))''')
    curse.execute("CREATE TABLE IF NOT EXISTS junctionWorkxWit(locID integer PRIMARY KEY DEFAULT 0 NOT NULL, workID, witID, FOREIGN KEY(workID) REFERENCES works(onpID), FOREIGN KEY(witID) REFERENCES witnesses(onpID))")
    curse.execute("CREATE TABLE IF NOT EXISTS junctionWitxCit(locID integer PRIMARY KEY DEFAULT 0 NOT NULL, witID, citID, FOREIGN KEY(witID) REFERENCES witnesses(onpID), FOREIGN KEY(citID) REFERENCES citations(onpID))")
    conn.close()


def get_base() -> List[str]:
    with open("wordurls.txt", "r") as f:
        baseList = f.read().splitlines()
    return baseList


def get_word_urls(urls: List[str]) -> List:
    """This function returns a Letter to Lemma junction table. 
    This table conatains all the URLs for all the lemmata.
    Columns are: 'LetterID'(str) and 'LemmaID'(str)"""
    res = []
    work_to_do = len(urls)
    itcnt = 0
    for url in urls:
        page = requests.get(url)
        letterID = url.rsplit("?", 1)
        letterID = letterID[1]
        bigbowl = BeautifulSoup(page.content, "lxml", from_encoding='UTF-8', parse_only=goodBits)
        lemmings = bigbowl.find_all('li')
        for l in lemmings:
            lemming = l.a.get('href')
            res.append(lemming)
        itcnt += 1
        divide_result = itcnt/work_to_do
        print(f"Currently getting word ID URLs. Completed {divide_result * 100} %", end="\r")
    return res


def get_onp_page_data(onpID: str):
    page = requests.get(f"{BASE_URL}{onpID}")
    return page


def build_lemma_tables():
    lem_id_list = get_word_urls(get_base())
    conn = create_connection()
    curse = conn.cursor()
    work_to_do = len(lem_id_list)
    itcnt = 0
    for i in lem_id_list:
        lem = get_onp_page_data(i)
        soup = BeautifulSoup(lem.content, "lxml", from_encoding='UTF-8')
        closing_in = soup.find_all("h2", "onp-title")
        lemma = closing_in[0].get("data-onorm")
        curse.execute("INSERT OR IGNORE INTO lemmata(onpID, lemma) VALUES(?, ?)", (i, lemma))
        soup = BeautifulSoup(lem.content, "lxml", from_encoding='UTF-8', parse_only=wordStrainer)
        subsoup = soup.find_all("li")
        for ii in subsoup:
            citron = ii.find("a")
            cit = citron.get("href")
            curse.execute("INSERT OR IGNORE INTO citations(onpID, citation) VALUES(?, ?)", (cit, cit))
            curse.execute("INSERT OR IGNORE INTO junctionLemxCit(lemID, citID) VALUES(?, ?)", (i, cit))
        itcnt +=1
        divide_result = itcnt/work_to_do
        print(f"Completed {divide_result * 100} % of lemmata", end='\r')
    conn.commit()
    conn.close()


def get_ms_id_list(inURL: str = START_MSS):
    conn = create_connection()
    curse = conn.cursor()
    page  = requests.get(inURL)
    soup = BeautifulSoup(page.content, "lxml", from_encoding="UTF-8", parse_only=goodBits)
    msIdList = soup.find_all("li")
    res = []
    for i in msIdList:
        onpID = i.a.get('href')
        nameRaw = i.get_text()
        msName, msDateRaw = nameRaw.split('(', 1)
        res.append((onpID, msName))
    curse.executemany("INSERT OR IGNORE INTO manuscripts(onpID, shelfmark) VALUES (?, ?)", res)
    conn.commit()
    conn.close()


def get_works(inURL: str = START_WORKS):
    conn = create_connection()
    curse = conn.cursor()
    page  = requests.get(inURL)
    soup = BeautifulSoup(page.content, "lxml", from_encoding="UTF-8", parse_only=goodBits)
    workList = soup.find_all("li")
    for i in workList:
        onpID = i.a.get('href')
        nameRaw = i.get_text()
        abbr, name = nameRaw.split("-", 1)
        curse.execute("INSERT OR IGNORE INTO works(onpID, name, abbreviation) VALUES (?, ?, ?)", (onpID, f"{name.strip()}", f"{abbr.strip()}"))
    conn.commit()
    conn.close() 


def get_witnesses():
    """
    Gets the information on the individual text witnesses from ONP (IDs beginning with ?r)
    and writes two tables: 
    """
    with create_connection() as conn:
        curse = conn.cursor()
        curse.execute("SELECT onpID FROM works")
        workList = [x[0] for x in curse.fetchall()]
    res = []
    res1 = []
    conn = create_connection()
    curse = conn.cursor()
    for i in workList:
        page = get_onp_page_data(i)
        soup = BeautifulSoup(page.content, "lxml", from_encoding="UTF-8", parse_only=anotherListStrainer)
        witList = soup.find_all("a")
        for ii in witList:
            onpID = ii.get('href')
            name = ii.span.get_text()
            res.append((onpID, name))
            res1.append((i, onpID))
            curse.execute("INSERT OR IGNORE INTO witnesses(onpID, name) VALUES (?, ?)", (onpID, name))
            curse.execute("INSERT OR IGNORE INTO junctionWorkxWit(workID, witID) VALUES (?, ?)", (i, onpID))
    conn.commit()
    conn.close()


def get_witcits():
    conn = create_connection()
    curse = conn.cursor()
    curse.execute("SELECT onpID FROM witnesses")
    work_list = [x[0] for x in curse.fetchall()]
    work_to_do = len(work_list)
    itcnt = 0
    for i in work_list:
        page = get_onp_page_data(i)
        soup = BeautifulSoup(page.content, "lxml", from_encoding="UTF-8", parse_only=goodBits)
        list_list = soup.find_all("li")
        for ii in list_list:
            onpID = ii.a.get('href')
            curse.execute("INSERT OR IGNORE INTO junctionWitxCit(witID, citID) VALUES (?, ?)", (i, onpID))
        itcnt +=1
        divide_result = itcnt/work_to_do
        print(f"Completed {divide_result * 100} % of citations for witnesses", end='\r')
    conn.commit()
    conn.close()


def build_LemWit_junction():
    with create_connection() as conn:
        df = pd.read_sql("SELECT a.lemID, b.witID FROM junctionLemxCit as a, junctionWitxCit as b WHERE a.citID = b.citID", conn)
        df.to_sql("junctionLemxWit", conn)


def get_ms_contents():
    with create_connection() as conn:
        df = pd.read_sql("SELECT * FROM manuscripts", conn)
        msList0 = df["onpID"].to_list()
        try:
            df = pd.read_sql("SELECT * FROM msInfo", conn)
            gotList = df["onpID"].to_list()
            msList = [x for x in msList0 if x not in gotList]
        except:
            print("Nothing in DB yet.")
            msList = msList0
    msdata = []
    juncMsxWit = []
    jobnum = len(msList)
    itcnt = 0
    itcnt1 = 0
    print(f"Crawling a total of {jobnum} mss.")
    timer = 0
    for i in msList:
        start = time.time()
        page = get_onp_page_data(i)
        soup = BeautifulSoup(page.content, features="lxml")
        croutons = soup.find(class_="list-group mb-3")
        crumbs = croutons.find_all("a")
        for ii in crumbs:
            witID = ii.get("href")
            juncMsxWit.append((i, witID))
        pfields = soup.find_all("p")
        shitty = soup.find("ul")
        shitty.decompose()
        stuffy = soup.find(class_="onp-title")
        stuff0 = stuffy.get_text()
        stuff1 = stuff0.strip()
        stuff2 = pfields[2].get_text()
        stuff2 = stuff2.strip()
        try:
            if "-" in stuff2:
                if "," in stuff2:
                    shit = stuff2.split(",")
                    if "1" and "-" in shit[0]:
                        stuff2 = shit[0]
                    else:
                        stuff2 = [x for x in shit if "-" in x]
                        stuff2 = stuff2[0]
                post, ante = stuff2.split("-")
                if "&" in ante:
                    _, ante = ante.split("&")
            elif "," in stuff2:
                dates = stuff2.split(",")
                post = dates[0]
                ante = dates[-1]
            elif "&" in stuff2:
                post, ante = stuff2.split("&")
            else:
                post = stuff2
                ante = stuff2
            post = "".join([x for x in post if x.isdigit()])
            ante = "".join([x for x in ante if x.isdigit()])
            if not ante:
                ante = post
            if not post:
                post = ante
            if not ante:
                ante = 9999
            if not post:
                post = 9999
            msdata.append((i, stuff1, int(post), int(ante)))
        except:
            import pdb; pdb.set_trace()
        itcnt += 1
        itcnt1 += 1
        rmnj = jobnum - itcnt1
        end = time.time()
        elpsd = end - start
        timer += elpsd
        if itcnt == 100 or rmnj == 0:
            print(f"Did 100 MSs in {timer} seconds")
            print(f"Finished {itcnt1/jobnum} %")
            print(f"Remaining ETA: {timer*rmnj/100/60} Minutes")
            timer = 0
            conn = create_connection()
            curse = conn.cursor()
            curse.execute("CREATE TABLE IF NOT EXISTS msInfo(locID integer PRIMARY KEY DEFAULT 0 NOT NULL, onpID UNIQUE, shelfmark, postquem, antequem)")
            curse.executemany("INSERT OR IGNORE INTO msInfo(onpID, shelfmark, postquem, antequem) VALUES (?, ?, ?, ?)", msdata)
            conn.commit()
            curse.execute("""CREATE TABLE IF NOT EXISTS 
                    junctionMsxWitreal(locID integer PRIMARY KEY DEFAULT 0 NOT NULL, 
                    msID, 
                    witID,
                    FOREIGN KEY (msID) REFERENCES msInfo(onpID),
                    FOREIGN KEY (witID) REFERENCES witnesses(onpID))""")
            curse.executemany("INSERT OR IGNORE INTO junctionMsxWitreal(msID, witID) VALUES (?, ?)", juncMsxWit)
            conn.commit()
            conn.close()
            itcnt = 0
            msdata = []
    conn = create_connection()
    df1 = pd.read_sql("SELECT * FROM msInfo", conn)
    df2 = pd.read_sql("SELECT * FROM junctionMsxWitreal", conn)


def main():
    db_setup(create_connection())
    print("DB created/connected. Getting lemmata next.")
    build_lemma_tables()
    print("Got lemmata. Doing MSs next.")
    get_ms_id_list()
    print("Got MS ids. Doing textworks next.")
    get_works()
    print("Got textworks. Doing textwitnesses next.")
    get_witnesses()
    print("Got all the witnesses. Now getting all the citations from the witnesses.")
    get_witcits()
    print("Got the citations. Now joing lemma-citations and witness-citations to get the lemma to witness")
    build_LemWit_junction()
    print("It appears we are done. Lets hope we got it all.")


if __name__ == '__main__':
   main()
   get_ms_contents()
   