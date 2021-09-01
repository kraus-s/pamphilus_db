import streamlit as st
import wordsorter as rawMaterials
from wordsorter import easy_bake_oven as lnm
import pandas as pd



if __name__ == '__main__':
    st.write('Pamph-O-Mat v0.1')
    df = lnm()
    st.multiselect()
    