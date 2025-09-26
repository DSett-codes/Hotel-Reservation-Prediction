import os 
import pandas as pd
from google.cloud import storage
from sklearn.model_selection import train_test_split
from src.logger import get_logger
from src.exception import CustomException
from config.paths_config import *
from utils.common_fucntions import read_yaml
import sys

logger = get_logger(__name__)   

class DataIngestion:
    def __init__(self,config) -> None:
        self.config = config['data_ingestion']
        self.bucket_name = self.config['bucket_name']
        self.file_name = self.config['bucket_file_name']
        self.train_test_ratio = self.config['train_ratio']
        self.project_id = self.config['project_id']
  

        os.makedirs(RAW_DIR,exist_ok=True)
        logger.info(f"Data Ingestion started with {self.bucket_name} and file name is {self.file_name}")

    def download_csv_from_gcp(self):
        try:
            client = storage.Client(project=self.project_id)
            bucket = client.bucket(self.bucket_name)
            blob = bucket.blob(self.file_name)
            blob.download_to_filename(RAW_FILE_PATH)
            logger.info(f"Data downloaded from GCS to {RAW_FILE_PATH}")
        except Exception as e:
            logger.error(f"Error downloading data from GCS: {str(e)}")
            raise CustomException(e, sys)
        
    def split_data(self):
        try:
            df = pd.read_csv(RAW_FILE_PATH)
            train, test = train_test_split(df, test_size=self.test_ratio, random_state=42)
            train.to_csv(TRAIN_FILE_PATH, index=False)
            test.to_csv(TEST_FILE_PATH, index=False)
            logger.info(
                f"Data split into train and test with ratio {self.train_ratio:.2f}/{self.test_ratio:.2f}"
            )
        except Exception as e:
            logger.error(f"Error splitting data: {str(e)}")
            raise CustomException(e, sys)
    
    def run(self):
        try:
            logger.info("Data ingestion started")
            self.download_csv_from_gcp()
            self.split_data()
            logger.info("Data ingestion completed")
        except Exception as e:
            logger.error(f"Custom Exception: {str(e)}")
            raise CustomException(e, sys)

if __name__ == "__main__":
    data_ingestion = DataIngestion(read_yaml(CONFIG_PATH))
    data_ingestion.run()
