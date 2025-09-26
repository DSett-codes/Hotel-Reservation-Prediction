import os
import pandas 
from src.logger import get_logger 
from src.exception import CustomException
import yaml

logger = get_logger(__name__)

def read_yaml(file_path) -> dict:
    try:
        with open(file_path) as yaml_file:
            config = yaml.safe_load(yaml_file)
        logger.info(f"yaml file: {file_path} loaded successfully")
        return config
    except Exception as e:
        logger.error(f"Error while reading yaml file: {file_path}")
        raise CustomException(e)