import hydra
from omegaconf import DictConfig

from src.main import create_dashboards


@hydra.main(config_path="./conf", config_name="prod", version_base=None)
def main(config: DictConfig):
    # all initialization work here
    create_dashboards(config)


if __name__ == '__main__':
    main()
