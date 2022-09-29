import hydra
from omegaconf import DictConfig

from src.extract import download


@hydra.main(config_path="./conf", config_name="prod", version_base=None)
def main(config: DictConfig):
    download(config)


if __name__ == '__main__':
    main()
