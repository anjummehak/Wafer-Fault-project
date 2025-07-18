import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer

class Preprocessor:
    """
    This class is responsible for cleaning and transforming data before training.
    """

    def __init__(self, file_object, logger_object):
        """
        Initializes Preprocessor with file and logger objects.

        Args:
            file_object: File object for logging.
            logger_object: Logger object to log messages.
        """
        self.file_object = file_object
        self.logger_object = logger_object

    def remove_columns(self, data, columns):
        """
        Removes specified columns from the dataset.

        Args:
            data: DataFrame from which columns need to be removed.
            columns: List of column names to remove.

        Returns:
            A DataFrame after removing the specified columns.

        Raises:
            Exception: If there is an error in column removal.
        """
        self.logger_object.log(self.file_object, 'Entered remove_columns method.')
        try:
            useful_data = data.drop(labels=columns, axis=1)  # Remove specified columns
            self.logger_object.log(self.file_object, 'Column removal successful.')
            return useful_data
        except Exception as e:
            self.logger_object.log(self.file_object, f"Error in remove_columns: {str(e)}")
            raise Exception("Column removal failed.")

    def separate_label_feature(self, data, label_column_name):
        """
        Separates features and label columns.

        Args:
            data: The dataset containing features and label columns.
            label_column_name: The name of the label column.

        Returns:
            X: Features DataFrame.
            Y: Label column DataFrame.

        Raises:
            Exception: If label separation fails.
        """
        self.logger_object.log(self.file_object, 'Entered separate_label_feature method.')
        try:
            X = data.drop(labels=label_column_name, axis=1)  # Features
            Y = data[label_column_name]  # Labels
            self.logger_object.log(self.file_object, 'Label separation successful.')
            return X, Y
        except Exception as e:
            self.logger_object.log(self.file_object, f"Error in separate_label_feature: {str(e)}")
            raise Exception("Label separation failed.")

    def is_null_present(self, data):
        """
        Checks if there are any null values in the dataset.

        Args:
            data: DataFrame to check for null values.

        Returns:
            True if null values are present, False otherwise.

        Raises:
            Exception: If the null check fails.
        """
        self.logger_object.log(self.file_object, 'Entered is_null_present method.')
        try:
            null_counts = data.isna().sum()
            null_present = null_counts.any()  # Check if any column has null values

            if null_present:
                dataframe_with_null = pd.DataFrame({
                    'columns': data.columns,
                    'missing values count': null_counts
                })
                dataframe_with_null.to_csv('preprocessing_data/null_values.csv')  # Save missing values info to a file

            self.logger_object.log(self.file_object, 'Null check completed.')
            return null_present
        except Exception as e:
            self.logger_object.log(self.file_object, f"Error in is_null_present: {str(e)}")
            raise Exception("Null check failed.")

    def impute_missing_values(self, data):
        """
        Imputes missing values in the dataset using KNN Imputer.

        Args:
            data: DataFrame with missing values.

        Returns:
            A DataFrame with missing values imputed.

        Raises:
            Exception: If imputation fails.
        """
        self.logger_object.log(self.file_object, 'Entered impute_missing_values method.')
        try:
            imputer = KNNImputer(n_neighbors=3, weights='uniform', missing_values=np.nan)
            new_array = imputer.fit_transform(data)  # Impute missing values
            new_data = pd.DataFrame(data=new_array, columns=data.columns)
            self.logger_object.log(self.file_object, 'Missing values imputed successfully.')
            return new_data
        except Exception as e:
            self.logger_object.log(self.file_object, f"Error in impute_missing_values: {str(e)}")
            raise Exception("Imputation of missing values failed.")

    def get_columns_with_zero_std_deviation(self, data):
        """
        Finds columns in the dataset that have a standard deviation of zero.

        Args:
            data: DataFrame to check for zero standard deviation columns.

        Returns:
            List of columns with zero standard deviation.

        Raises:
            Exception: If the process fails.
        """
        self.logger_object.log(self.file_object, 'Entered get_columns_with_zero_std_deviation method.')
        try:
            cols_with_zero_std = data.columns[data.std() == 0].tolist()  # Find columns with zero std deviation
            self.logger_object.log(self.file_object, 'Zero standard deviation column check successful.')
            return cols_with_zero_std
        except Exception as e:
            self.logger_object.log(self.file_object, f"Error in get_columns_with_zero_std_deviation: {str(e)}")
            raise Exception("Column search for zero standard deviation failed.")
