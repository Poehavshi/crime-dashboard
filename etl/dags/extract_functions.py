import requests
import logging
import pandas as pd

DATA_SOURCES = {"climate_by_country.csv": "https://query.data.world/s/euo44nk2cizrjavkfqtogk7eiwwv4u",
                "climate_by_state.csv": "https://query.data.world/s/epcm3iauwcborpfhda7en6yeqdgsux",
                "geo_climate.geojson": "http://cecgis-caenergy.opendata.arcgis.com/datasets/549017ee96e341d2bbb3dd0c291a9112_0.geojson"
                }


def extract_climate():
    for filename, url in DATA_SOURCES.items():
        try:
            response = requests.get(url)
            with open(filename, "wb") as file:
                file.write(response.content)
                logging.info(f"Create {filename} file")
        except requests.exceptions.RequestException as e:
            logging.critical(f"ERROR when download climate data: {e}")


def transform_and_load_climate():
    climate_by_state = pd.read_csv("climate_by_state.csv")
    climate_by_state_usa = climate_by_state[climate_by_state['Country'] == 'United States'].drop('Country', axis=1)
    climate_by_state_usa = climate_by_state_usa.reindex(
        columns=['index', 'dt', 'day', 'month', 'year', 'State', 'AverageTemperature', 'AverageTemperatureUncertainty'])
    climate_by_state_usa["dt"] = pd.to_datetime(climate_by_state_usa["dt"])
    climate_by_state_usa.reset_index(inplace=True)
    climate_by_state_usa['day'] = climate_by_state_usa["dt"].dt.day
    climate_by_state_usa['month'] = climate_by_state_usa["dt"].dt.month
    climate_by_state_usa['year'] = climate_by_state_usa["dt"].dt.year
    climate_by_state_usa.rename(columns={'index': 'unique_key'}, inplace=True)
    # todo create postgres connection and load into it
