from bs4 import BeautifulSoup
import pandas as pd
from util import latin_para_import





def apply_sort(inDF: pd.DataFrame, currMS: str) -> pd.DataFrame:
    orderPD = pd.read_excel("latMat/verseorder.xlsx" ,index_col=None, usecols=['Base', currMS])
    orderPD[currMS] = orderPD[currMS].astype(str)
    orderPD['Base'] = orderPD['Base'].astype(int)
    orderPD.dropna(inplace=True)
    newDF = pd.merge(left=inDF, right=orderPD, left_on='Verse', right_on=currMS, how='left')
    newDF.set_index('Base', inplace=True)
    newDF = newDF.sort_index()
    newDF = newDF.drop(columns=currMS)
    return newDF


def clean_return(infile):
    witDict = latin_para_import(infile, outputDF="dict")
    for key, value in witDict.items():
        newDF = apply_sort(value, key)
        witDict.update(key = newDF)
    return


if __name__ == "__main__":
    clean_return("latMat/pamphLat.xml")