from utils import dbBuilder
import requests
import os
from utils.constants import *
from utils import stylo
from utils import onpnode2vec
from utils import msclustering
from tqdm import tqdm

def download_onp_data():
    print("Downloading ONP data...")
    try:
        # Send an HTTP GET request to the URL
        response = requests.get(ONP_DB_URL, stream=True)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            with open(ONP_DATABASE_PATH, 'wb') as file:
                total_length = int(response.headers.get('content-length'))
                with tqdm(total=total_length, unit='B', unit_scale=True) as pbar:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            file.write(chunk)
                            pbar.update(len(chunk))
            print(f"File downloaded and saved as '{ONP_DATABASE_PATH}'")
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


def download_levenshtein_data():
    print("Downloading levenshtein data...")
    try:
        # Send an HTTP GET request to the URL
        response = requests.get(LEVENSHTEIN_DB_URL)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            print("Downloading...")
            with open(LEVEN_DB, 'wb') as file:
                total_length = int(response.headers.get('content-length'))
                with tqdm(total=total_length, unit='B', unit_scale=True) as pbar:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            file.write(chunk)
                            pbar.update(len(chunk))
            print(f"File downloaded and saved as '{LEVEN_DB}'")
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    download_onp_data()
    # dbBuilder.main()
    # Not needed at this time TODO: Clean up
    print("App setup successfull")
    print("You can run stylo, node2vec and clustering from here. Warning! Node2vec takes about a week on an M1 Pro with 32GB of RAM.")
    print("All the data is either shipped with the app or downloaded during setup.")
    print("If you chose not to run the stylometry, data will be downloaded instead.")
    run_stylometry = input("Run stylometry? (y/n): ")
    if run_stylometry == "y":
        stylo.run()
    elif run_stylometry == "n":
        download_levenshtein_data()
    run_node2vec = input("Run node2vec? (y/n): ")
    if run_node2vec == "y":
        onpnode2vec.run()
    run_clustering = input("Run clustering? (y/n): ")
    if run_clustering == "y":
        msclustering.main()
    print("All done. Bye!")