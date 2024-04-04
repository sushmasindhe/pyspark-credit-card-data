import logging
import json


class CommonUtils:
    @staticmethod
    def create_logger():
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        return logger

    @staticmethod
    def json_reader(path):
        with open(path, 'r') as file:
            config = json.load(file)
        return config
