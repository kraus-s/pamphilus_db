import streamlit as st
import wordsorter as rawMaterials
from wordsorter import easy_bake_oven as lnm
import pandas as pd
import plotly.graph_objects as go





if __name__ == '__main__':
    st.write('Pamph-O-Mat v0.1')
    df = lnm()
    
    pamphSelect = list(df.columns)
    showWhat = st.multiselect(label="Spaltenauswahl", options=pamphSelect, default=pamphSelect)
    df2 = df[showWhat]
    st.write(df2)


    