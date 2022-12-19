import requests
import logging


def download(filename, url):
    try:
        response = requests.get(url)
        with open(filename, "wb") as file:
            file.write(response.content)
            logging.info(f"Create {filename} file")
    except requests.exceptions.RequestException as e:
        logging.critical(f"ERROR when download data: {e}")
