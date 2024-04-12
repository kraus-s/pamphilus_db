import pandas as pd
from bs4 import BeautifulSoup
import os
import pickle
from utils.menota_parser import NorseDoc
from utils.constants import *
import glob
import utils.menota_parser as menota_parser


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
    

def load_data() -> list[NorseDoc]:
    if not os.path.exists(MENOTA_COMPLETE_PICKLE):
        parsed_docs_list = import_menota_data(path= OLD_NORSE_CORPUS_FILES)
        pickle.dump(parsed_docs_list, open(MENOTA_COMPLETE_PICKLE, "wb"))
    else:
        parsed_docs_list = pickle.load(open(MENOTA_COMPLETE_PICKLE, "rb"))
    return parsed_docs_list


def import_menota_data(path: str = OLD_NORSE_CORPUS_FILES) -> list[NorseDoc]:
    """
    Imports the Norse corpus from the Menota XML files
    """
    path_list = glob.glob(f"{path}*.xml")
    docs_to_parse = path_list
    entities = menota_parser.download_and_parse_entities("http://www.menota.org/menota-entities.txt")
    parsed_docs_list = [x for path in docs_to_parse for x in menota_parser.get_regular_text(path, entities)]

    return parsed_docs_list