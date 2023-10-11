import pandas as pd
from bs4 import BeautifulSoup


def onp_dataset() -> pd.DataFrame:
    """Reads the ONP coocurrence dataset from local html file into a df.

    Args:
        None
    
    Returns:
        pd.DataFrame
        Dataframe columns:
            - 'lemma'
            - 'other'
    """
    resultsONP = open('./pamph-lemmata-cooccurrences.html', 'r', encoding="UTF-8")
    onpResultDFList = pd.read_html(resultsONP, encoding="UTF-8")
    onpResultDF = onpResultDFList[0]
    onpResultDF['lemma'] = onpResultDF['lemma'].str.strip('123456789')
    return onpResultDF


def read_tei(tei_file):
    with open(tei_file, 'r', encoding="UTF-8") as tei:
        soup = BeautifulSoup(tei, from_encoding='UTF-8', features='lxml-xml')
        return soup