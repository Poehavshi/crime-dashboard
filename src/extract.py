import os
from torchvision.datasets.utils import download_url

import hydra
import requests
from omegaconf import DictConfig
from pathlib import Path


def download(config: DictConfig):
    for dataset in config.get("data_source"):
        # 1) create directory
        dataset_path = Path(config.cache_path).joinpath(dataset)
        os.makedirs(dataset_path, exist_ok=True)

        for url in config.get("data_source").get(dataset):
            print(url)
            download_url(url, str(dataset_path))
