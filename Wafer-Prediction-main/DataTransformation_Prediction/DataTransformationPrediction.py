from datetime import datetime
from os import listdir
from pathlib import Path
import pandas as pd
from application_logging.logger import App_Logger


class DataTransformPredict:
    """
    This class is responsible for transforming the Good Raw Prediction Data before loading it into the database.

    Attributes:
        good_data_path (Path): Path to the directory containing validated good raw prediction data files.
        logger (App_Logger): Logger instance for logging messages.
    """

    def __init__(self):
        self.good_data_path = Path("Prediction_Raw_Files_Validated/Good_Raw")
        self.logger = App_Logger()

    def replace_missing_with_null(self) -> None:
        """
        Replaces missing values in columns with "NULL" and modifies the 'Wafer' column to keep only integer data.

        This method processes all CSV files in the `good_data_path` directory, replaces missing values with "NULL",
        and trims the 'Wafer' column to retain only the integer portion. The transformed data is saved back to the same file.

        Raises:
            Exception: If any error occurs during the transformation process.
        """
        log_file = open("Prediction_Logs/dataTransformLog.txt", 'a+')
        try:
            for file in listdir(self.good_data_path):
                file_path = self.good_data_path / file
                df = pd.read_csv(file_path)

                # Replace missing values with "NULL"
                df.fillna('NULL', inplace=True)

                # Trim the 'Wafer' column to keep only the integer portion
                df['Wafer'] = df['Wafer'].str[6:]

                # Save the transformed data back to the same file
                df.to_csv(file_path, index=None, header=True)
                self.logger.log(log_file, f"{file}: File transformed successfully!!")

        except Exception as e:
            self.logger.log(log_file, f"Data transformation failed because: {e}")
            raise e  # Re-raise the exception after logging
        finally:
            log_file.close()


# Example usage
if __name__ == "__main__":
    transformer = DataTransformPredict()
    transformer.replace_missing_with_null()