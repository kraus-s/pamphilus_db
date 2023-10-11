import dbBuilder
import requests
import os
from constants import *

def download_onp_data():


    try:
        # Send an HTTP GET request to the URL
        response = requests.get(ONP_DB_URL)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            with open(ONP_DATABASE_PATH, 'wb') as file:
                file.write(response.content)
            print(f"File downloaded and saved as '{ONP_DATABASE_PATH}'")
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    dbBuilder.main()
    download_onp_data()
    print("App setup successfull")