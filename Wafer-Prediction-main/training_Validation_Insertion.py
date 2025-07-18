from datetime import datetime
from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from DataTransform_Training.DataTransformation import dataTransform
from application_logging import logger


class TrainValidation:
    def __init__(self, path):
        """Initializes the train validation object with path."""
        self.raw_data = Raw_Data_validation(path)
        self.data_transform = dataTransform()
        self.db_operation = dBOperation()
        self.file_object = open("Training_Logs/Training_Main_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()

    def _log_validation_start(self):
        """Logs the start of the validation process."""
        self.log_writer.log(self.file_object, 'Start of Validation on files!!')

    def _extract_values_from_schema(self):
        """Extract values like date stamp length, column names, etc., from schema."""
        return self.raw_data.valuesFromSchema()

    def _validate_filename(self, regex, date_stamp_length, time_stamp_length):
        """Validates the filename using regex."""
        self.raw_data.validationFileNameRaw(regex, date_stamp_length, time_stamp_length)

    def _validate_columns(self, column_count):
        """Validates the column length in the training file."""
        self.raw_data.validateColumnLength(column_count)

    def _validate_missing_values(self):
        """Validates if any column has missing values."""
        self.raw_data.validateMissingValuesInWholeColumn()

    def _transform_data(self):
        """Performs data transformation by replacing blanks with Null."""
        self.data_transform.replaceMissingWithNull()

    def _create_database_table(self, column_names):
        """Creates a training database and the corresponding table with schema."""
        self.db_operation.createTableDb('Training', column_names)

    def _insert_data_into_table(self):
        """Inserts data from the training file into the database table."""
        self.db_operation.insertIntoTableGoodData('Training')

    def _delete_good_data_folder(self):
        """Deletes the good data folder after successful data insertion."""
        self.raw_data.deleteExistingGoodDataTrainingFolder()

    def _move_bad_files_to_archive(self):
        """Moves the bad files to the archive folder."""
        self.raw_data.moveBadFilesToArchiveBad()

    def _export_data_to_csv(self):
        """Exports data from the database table into a CSV file."""
        self.db_operation.selectingDatafromtableintocsv('Training')

    def train_validation(self):
        """Main function to handle the training validation workflow."""
        try:
            self._log_validation_start()

            # Step 1: Extract values from schema and validate file
            date_stamp_length, time_stamp_length, column_names, column_count = self._extract_values_from_schema()
            regex = self.raw_data.manualRegexCreation()
            self._validate_filename(regex, date_stamp_length, time_stamp_length)

            # Step 2: Validate columns and missing values
            self._validate_columns(column_count)
            self._validate_missing_values()

            self.log_writer.log(self.file_object, "Raw Data Validation Complete!!")

            # Step 3: Data Transformation
            self.log_writer.log(self.file_object, "Starting Data Transformation!!")
            self._transform_data()
            self.log_writer.log(self.file_object, "Data Transformation Completed!!!")

            # Step 4: Create Database and Tables
            self.log_writer.log(self.file_object, "Creating Training_Database and tables based on schema!!")
            self._create_database_table(column_names)
            self.log_writer.log(self.file_object, "Table creation Completed!!")

            # Step 5: Insert data into table
            self.log_writer.log(self.file_object, "Insertion of Data into Table started!!")
            self._insert_data_into_table()
            self.log_writer.log(self.file_object, "Insertion in Table completed!!")

            # Step 6: Clean up after successful insertion
            self._delete_good_data_folder()
            self._move_bad_files_to_archive()

            self.log_writer.log(self.file_object, "Validation Operation completed!!")
            self.log_writer.log(self.file_object, "Extracting csv file from table")

            # Step 7: Export data to CSV
            self._export_data_to_csv()

            self.file_object.close()

        except Exception as e:
            self.log_writer.log(self.file_object, f"Error occurred during validation: {e}")
            raise e


# Example usage:
# path = "your_file_path_here"
# validation = TrainValidation(path)
# validation.train_validation()
