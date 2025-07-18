import pandas as pd

class DataGetterPred:
    """
    This class is used for obtaining the data from the source for prediction.
    """
    def __init__(self, file_object, logger_object):
        self.prediction_file = 'Prediction_FileFromDB/InputFile.csv'
        self.file_object = file_object
        self.logger_object = logger_object

    def get_data(self):
        """
        Reads the data from the source and returns it as a pandas DataFrame.
        """
        self.logger_object.log(self.file_object, 'Entered the get_data method of DataGetterPred')
        try:
            data = pd.read_csv(self.prediction_file)
            self.logger_object.log(self.file_object, 'Data Load Successful. Exited get_data method.')
            return data
        except Exception as e:
            self.logger_object.log(self.file_object, f'Error in get_data: {str(e)}')
            self.logger_object.log(self.file_object, 'Data Load Unsuccessful. Exited get_data method.')
            raise
