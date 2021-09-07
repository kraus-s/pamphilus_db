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
    
    fig = go.Figure(data=[go.Table(
    header=dict(values=list(df2.columns),
                fill_color='grey',
                align='left'),
    cells=dict(values=[df2[col] for col in df2.columns],
               fill_color='lightgray',
               align='left'))
    ])

    st.plotly_chart(fig)    



    