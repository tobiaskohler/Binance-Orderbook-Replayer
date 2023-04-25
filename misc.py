import yaml


def load_config():
    with open('config.yaml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    return config 
