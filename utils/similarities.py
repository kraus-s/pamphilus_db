import os
import pandas as pd

def get_csv_filenames(directory: str = "data/similarities/stylo") -> list[str]:
    csv_files = []
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            csv_files.append(filename)
    return csv_files


def get_similarity(file_path: str):
    return pd.read_csv(file_path, index_col=0)