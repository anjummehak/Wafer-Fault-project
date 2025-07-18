import os
import shutil
import re
import json
import pandas as pd
from datetime import datetime
from application_logging.logger import App_Logger

class RawDataValidation:
    """
    Handles validation of raw training data.
    Validates file names, column lengths, missing values, and more based on a predefined schema.
    """

    def __init__(self, path):
        self.batch_directory = path
        self.schema_path = 'schema_training.json'
        self.logger = App_Logger()

    def _log_message(self, log_file, message):
        with open(log_file, 'a+') as log:
            self.logger.log(log, message)

    def _create_directory(self, directory):
        if not os.path.isdir(directory):
            os.makedirs(directory)

    def _delete_directory(self, directory):
        if os.path.isdir(directory):
            shutil.rmtree(directory)

    def _move_files(self, src, dest, files):
        for file in files:
            shutil.move(os.path.join(src, file), dest)

    def _load_schema(self):
        try:
            with open(self.schema_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self._log_message("Training_Logs/GeneralLog.txt", f"Error loading schema: {e}")
            raise

    def values_from_schema(self):
        """
        Extracts values from schema file for validation (e.g., date, timestamp length, columns).
        """
        try:
            schema = self._load_schema()
            LengthOfDateStampInFile = schema['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = schema['LengthOfTimeStampInFile']
            column_names = schema['ColName']
            NumberofColumns = schema['NumberofColumns']

            message = f"DateStamp Length: {LengthOfDateStampInFile}, TimeStamp Length: {LengthOfTimeStampInFile}, Columns: {NumberofColumns}"
            self._log_message("Training_Logs/valuesfromSchemaValidationLog.txt", message)
            return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns
        except KeyError as e:
            self._log_message("Training_Logs/valuesfromSchemaValidationLog.txt", f"KeyError: {e}")
            raise

    def manual_regex_creation(self):
        """
        Returns a regex pattern for validating file names.
        """
        return r"['wafer']+['\_'']+[\d_]+[\d]+\.csv"

    def create_directories_for_good_bad_raw_data(self):
        """
        Creates directories to store validated good and bad raw data.
        """
        self._create_directory("Training_Raw_files_validated/Good_Raw/")
        self._create_directory("Training_Raw_files_validated/Bad_Raw/")

    def delete_existing_directories(self):
        """
        Deletes existing directories for good and bad data to optimize space.
        """
        self._delete_directory('Training_Raw_files_validated/Good_Raw/')
        self._delete_directory('Training_Raw_files_validated/Bad_Raw/')

    def move_bad_files_to_archive(self):
        """
        Archives bad data files and removes the bad raw data folder.
        """
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        archive_path = f"TrainingArchiveBadData/BadData_{timestamp}"

        self._create_directory(archive_path)
        bad_files = os.listdir('Training_Raw_files_validated/Bad_Raw/')
        self._move_files('Training_Raw_files_validated/Bad_Raw/', archive_path, bad_files)

        self._delete_directory('Training_Raw_files_validated/Bad_Raw/')
        self._log_message("Training_Logs/GeneralLog.txt", "Bad files moved to archive.")

    def validate_file_name(self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile):
        """
        Validates the file name according to schema and moves files to appropriate folders.
        """
        self.delete_existing_directories()
        self.create_directories_for_good_bad_raw_data()

        for filename in os.listdir(self.batch_directory):
            if re.match(regex, filename):
                split_at_dot = re.split('.csv', filename)[0]
                split_at_underscore = re.split('_', split_at_dot)
                if len(split_at_underscore[1]) == LengthOfDateStampInFile and len(split_at_underscore[2]) == LengthOfTimeStampInFile:
                    shutil.copy(os.path.join(self.batch_directory, filename), "Training_Raw_files_validated/Good_Raw")
                    self._log_message("Training_Logs/nameValidationLog.txt", f"Valid file: {filename}")
                else:
                    shutil.copy(os.path.join(self.batch_directory, filename), "Training_Raw_files_validated/Bad_Raw")
                    self._log_message("Training_Logs/nameValidationLog.txt", f"Invalid file: {filename}")
            else:
                shutil.copy(os.path.join(self.batch_directory, filename), "Training_Raw_files_validated/Bad_Raw")
                self._log_message("Training_Logs/nameValidationLog.txt", f"Invalid file name: {filename}")

    def validate_column_length(self, expected_columns):
        """
        Validates the number of columns in CSV files.
        """
        for file in os.listdir('Training_Raw_files_validated/Good_Raw/'):
            csv = pd.read_csv(os.path.join('Training_Raw_files_validated/Good_Raw/', file))
            if csv.shape[1] != expected_columns:
                shutil.move(os.path.join('Training_Raw_files_validated/Good_Raw/', file), 'Training_Raw_files_validated/Bad_Raw')
                self._log_message("Training_Logs/columnValidationLog.txt", f"Invalid column length: {file}")

    def validate_missing_values(self):
        """
        Validates if any column has missing values for the entire column.
        """
        for file in os.listdir('Training_Raw_files_validated/Good_Raw/'):
            csv = pd.read_csv(os.path.join('Training_Raw_files_validated/Good_Raw/', file))
            for column in csv.columns:
                if csv[column].isnull().all():
                    shutil.move(os.path.join('Training_Raw_files_validated/Good_Raw/', file), 'Training_Raw_files_validated/Bad_Raw')
                    self._log_message("Training_Logs/missingValuesInColumn.txt", f"Missing values in column: {file}")
                    break
            else:
                csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                csv.to_csv(os.path.join('Training_Raw_files_validated/Good_Raw/', file), index=False)
