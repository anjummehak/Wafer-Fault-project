import pandas
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from application_logging import logger
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation


class Prediction:

    def __init__(self, path):
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()

        if path:
            self.pred_data_val = Prediction_Data_validation(path)

    def _delete_existing_predictions(self):
        """Deletes the existing prediction file from last run."""
        self.pred_data_val.deletePredictionFile()

    def _log_start_of_prediction(self):
        """Logs the start of prediction process."""
        self.log_writer.log(self.file_object, 'Start of Prediction')

    def _load_data(self):
        """Loads the data for prediction."""
        data_getter = data_loader_prediction.Data_Getter_Pred(self.file_object, self.log_writer)
        return data_getter.get_data()

    def _preprocess_data(self, data):
        """Preprocesses the data to handle missing values and drop unnecessary columns."""
        preprocessor = preprocessing.Preprocessor(self.file_object, self.log_writer)
        
        if preprocessor.is_null_present(data):
            data = preprocessor.impute_missing_values(data)
        
        cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(data)
        return preprocessor.remove_columns(data, cols_to_drop)

    def _predict_with_kmeans(self, data):
        """Predicts clusters using the KMeans model."""
        file_loader = file_methods.File_Operation(self.file_object, self.log_writer)
        kmeans = file_loader.load_model('KMeans')
        return kmeans.predict(data.drop(['Wafer'], axis=1))

    def _predict_clusters(self, data, clusters):
        """Predicts and stores results for each cluster."""
        file_loader = file_methods.File_Operation(self.file_object, self.log_writer)
        path = "Prediction_Output_File/Predictions.csv"
        
        for cluster in clusters:
            cluster_data = data[data['clusters'] == cluster]
            wafer_names = list(cluster_data['Wafer'])
            cluster_data = cluster_data.drop(columns=['Wafer', 'clusters'])
            
            model_name = file_loader.find_correct_model_file(cluster)
            model = file_loader.load_model(model_name)
            
            result = list(model.predict(cluster_data))
            result_df = pandas.DataFrame(list(zip(wafer_names, result)), columns=['Wafer', 'Prediction'])
            result_df.to_csv(path, header=True, mode='a+')  # Appends result to prediction file
        
        return path, result_df.head().to_json(orient="records")

    def prediction_from_model(self):
        """Main function to handle prediction workflow."""
        try:
            self._delete_existing_predictions()
            self._log_start_of_prediction()

            # Step 1: Load data
            data = self._load_data()

            # Step 2: Preprocess the data
            data = self._preprocess_data(data)

            # Step 3: Perform KMeans prediction
            data['clusters'] = self._predict_with_kmeans(data)

            # Step 4: Get unique clusters and predict for each
            clusters = data['clusters'].unique()
            return self._predict_clusters(data, clusters)

        except Exception as ex:
            self.log_writer.log(self.file_object, f'Error occurred while running the prediction: {ex}')
            raise ex


# Example usage:
# prediction = Prediction(path)
# prediction.prediction_from_model()
