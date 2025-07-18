import os
import shutil
import re
import pandas as pd
import json
from datetime import datetime
from application_logging.logger import App_Logger


class PredictionDataValidator:
    """
    Class to validate the incoming prediction data files.
    It checks for file structure, column count, missing values, and filename format.
    """

    def __init__(self, raw_data_directory: str):
        self.raw_data_directory = raw_data_directory
        self.schema_path = 'schema_prediction.json'
        self.logger = App_Logger()

    def _read_schema(self):
        """
        Helper method to load schema data from JSON.
        Returns schema parameters.
        """
        with open(self.schema_path, 'r') as schema_file:
            schema_data = json.load(schema_file)
        
        return schema_data['LengthOfDateStampInFile'], schema_data['LengthOfTimeStampInFile'], schema_data['ColName'], schema_data['NumberofColumns']

    def _generate_file_regex(self):
        """
        Returns the regex pattern for validating file names.
        """
        return r"wafer_\d{4}_\d{6}\.csv"

    def _setup_directories(self):
        """
        Creates directories for storing good and bad data files.
        """
        os.makedirs('Prediction_Raw_Files_Validated/Good_Raw', exist_ok=True)
        os.makedirs('Prediction_Raw_Files_Validated/Bad_Raw', exist_ok=True)

    def _clear_directory(self, directory):
        """
        Deletes all files in a given directory.
        """
        if os.path.exists(directory):
            for file in os.listdir(directory):
                file_path = os.path.join(directory, file)
                if os.path.isfile(file_path):
                    os.remove(file_path)
    
    def _archive_bad_files(self):
        """
        Archives bad files into a separate folder for future reference.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archive_folder = f"PredictionArchivedBadData/BadData_{timestamp}"
        os.makedirs(archive_folder, exist_ok=True)

        bad_data_dir = 'Prediction_Raw_Files_Validated/Bad_Raw'
        for file in os.listdir(bad_data_dir):
            shutil.move(os.path.join(bad_data_dir, file), archive_folder)

    def _move_file(self, file_path, destination_dir):
        """
        Move a file to the specified directory.
        """
        shutil.copy(file_path, destination_dir)

    def validate_file_structure(self):
        """
        Validates the file structure by checking file names and timestamps.
        """
        regex = self._generate_file_regex()
        date_stamp_length, time_stamp_length, columns, num_columns = self._read_schema()

        self._setup_directories()
        self._clear_directory('Prediction_Raw_Files_Validated/Bad_Raw')

        for file_name in os.listdir(self.raw_data_directory):
            file_path = os.path.join(self.raw_data_directory, file_name)
            if re.match(regex, file_name):
                parts = file_name.split('_')
                if len(parts[1]) == date_stamp_length and len(parts[2].split('.')[0]) == time_stamp_length:
                    self._move_file(file_path, 'Prediction_Raw_Files_Validated/Good_Raw')
                    self.logger.log("Prediction_Logs/validation_log.txt", f"Valid file: {file_name}")
                else:
                    self._move_file(file_path, 'Prediction_Raw_Files_Validated/Bad_Raw')
                    self.logger.log("Prediction_Logs/validation_log.txt", f"Invalid timestamp: {file_name}")
            else:
                self._move_file(file_path, 'Prediction_Raw_Files_Validated/Bad_Raw')
                self.logger.log("Prediction_Logs/validation_log.txt", f"Invalid file format: {file_name}")

    def validate_column_count(self):
        """
        Checks if each file has the expected number of columns as per the schema.
        Moves invalid files to the bad data directory.
        """
        _, _, _, expected_column_count = self._read_schema()

        for file_name in os.listdir('Prediction_Raw_Files_Validated/Good_Raw'):
            file_path = os.path.join('Prediction_Raw_Files_Validated/Good_Raw', file_name)
            df = pd.read_csv(file_path)
            if df.shape[1] == expected_column_count:
                df.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                df.to_csv(file_path, index=False)
            else:
                self._move_file(file_path, 'Prediction_Raw_Files_Validated/Bad_Raw')
                self.logger.log("Prediction_Logs/validation_log.txt", f"Invalid column count: {file_name}")

    def validate_missing_values(self):
        """
        Validates the presence of missing values in the CSV files.
        Files with missing values are moved to the bad data directory.
        """
        for file_name in os.listdir('Prediction_Raw_Files_Validated/Good_Raw'):
            file_path = os.path.join('Prediction_Raw_Files_Validated/Good_Raw', file_name)
            df = pd.read_csv(file_path)
            if df.isnull().any().any():
                self._move_file(file_path, 'Prediction_Raw_Files_Validated/Bad_Raw')
                self.logger.log("Prediction_Logs/validation_log.txt", f"Missing values found: {file_name}")
            else:
                df.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                df.to_csv(file_path, index=False)

    def remove_existing_predictions(self):
        """
        Removes any existing prediction output file.
        """
        prediction_file = 'Prediction_Output_File/Predictions.csv'
        if os.path.exists(prediction_file):
            os.remove(prediction_file)
            self.logger.log("Prediction_Logs/validation_log.txt", "Existing predictions file removed.")

    def process_data(self):
        """
        Validates, cleans, and prepares data for prediction.
        """
        self.validate_file_structure()
        self.validate_column_count()
        self.validate_missing_values()
        self._archive_bad_files()
        self.remove_existing_predictions()
