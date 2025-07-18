import shutil
import sqlite3
from datetime import datetime
import os
import csv
from application_logging.logger import App_Logger


class DatabaseOperations:
    def __init__(self):
        self.base_path = 'Prediction_Database/'
        self.invalid_data_path = "Prediction_Raw_Files_Validated/Bad_Raw"
        self.valid_data_path = "Prediction_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()

    def connect_to_database(self, db_name):
        try:
            conn = sqlite3.connect(self.base_path + db_name + '.db')
            with open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+') as log_file:
                self.logger.log(log_file, f"Successfully connected to {db_name} database")
            return conn
        except ConnectionError as error:
            with open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+') as log_file:
                self.logger.log(log_file, f"Error while connecting to database: {error}")
            raise ConnectionError

    def create_table(self, db_name, column_definitions):
        try:
            conn = self.connect_to_database(db_name)
            conn.execute('DROP TABLE IF EXISTS Good_Raw_Data;')

            for column_name, data_type in column_definitions.items():
                try:
                    conn.execute(f'ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {data_type}')
                except:
                    conn.execute(f'CREATE TABLE Good_Raw_Data ({column_name} {data_type})')

            conn.close()
            with open("Prediction_Logs/DbTableCreateLog.txt", 'a+') as log_file:
                self.logger.log(log_file, "Table created successfully!")
            with open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+') as log_file:
                self.logger.log(log_file, f"Closed {db_name} database successfully")
        except Exception as error:
            with open("Prediction_Logs/DbTableCreateLog.txt", 'a+') as log_file:
                self.logger.log(log_file, f"Error while creating table: {error}")
            raise error

    def insert_good_data(self, db_name):
        try:
            conn = self.connect_to_database(db_name)
            log_file = open("Prediction_Logs/DbInsertLog.txt", 'a+')

            for file_name in os.listdir(self.valid_data_path):
                try:
                    with open(f"{self.valid_data_path}/{file_name}", "r") as file:
                        next(file)
                        reader = csv.reader(file, delimiter="\n")
                        for row in reader:
                            try:
                                conn.execute(f'INSERT INTO Good_Raw_Data VALUES ({",".join(row)})')
                                conn.commit()
                                self.logger.log(log_file, f"{file_name}: File loaded successfully!")
                            except Exception as insert_error:
                                conn.rollback()
                                raise insert_error
                except Exception as file_error:
                    shutil.move(f"{self.valid_data_path}/{file_name}", self.invalid_data_path)
                    self.logger.log(log_file, f"Error processing file {file_name}: {file_error}")
                    self.logger.log(log_file, f"Moved file {file_name} to bad data folder")
            conn.close()
            log_file.close()
        except Exception as error:
            with open("Prediction_Logs/DbInsertLog.txt", 'a+') as log_file:
                self.logger.log(log_file, f"Error inserting data: {error}")
            raise error

    def export_data_to_csv(self, db_name):
        export_path = 'Prediction_FileFromDB/'
        file_name = 'InputFile.csv'
        try:
            conn = self.connect_to_database(db_name)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM Good_Raw_Data")

            results = cursor.fetchall()
            headers = [description[0] for description in cursor.description]

            if not os.path.exists(export_path):
                os.makedirs(export_path)

            with open(f"{export_path}/{file_name}", 'w', newline='') as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=',', lineterminator='\r\n', quoting=csv.QUOTE_ALL, escapechar='\\')
                csv_writer.writerow(headers)
                csv_writer.writerows(results)

            with open("Prediction_Logs/ExportToCsv.txt", 'a+') as log_file:
                self.logger.log(log_file, "Data exported to CSV successfully!")
            conn.close()
        except Exception as error:
            with open("Prediction_Logs/ExportToCsv.txt", 'a+') as log_file:
                self.logger.log(log_file, f"Error exporting data to CSV: {error}")
            raise error
