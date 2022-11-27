import requests
import logging
import pandas as pd

DATA_SOURCES = {"climate.csv": "https://query.data.world/s/euo44nk2cizrjavkfqtogk7eiwwv4u",
                "climate2.csv": "https://query.data.world/s/epcm3iauwcborpfhda7en6yeqdgsux",
                "geo_climate.geojson": "http://cecgis-caenergy.opendata.arcgis.com/datasets/549017ee96e341d2bbb3dd0c291a9112_0.geojson"
                }


def download_climate():
    for filename, url in DATA_SOURCES.items():
        try:
            response = requests.get(url)
            with open(filename, "wb") as file:
                file.write(response.content)
                logging.info(f"Create {filename} file")
        except requests.exceptions.RequestException as e:
            logging.critical(f"ERROR when download climate data: {e}")


def show_climate_data():
    df = pd.read_csv("climate.csv")
    print(df)
    logging.info(df)
