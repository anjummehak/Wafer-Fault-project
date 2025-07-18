from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
from data_preprocessing import preprocessing
from data_preprocessing import clustering
from best_model_finder import tuner
from file_operations import file_methods
from application_logging import logger


class TrainModel:
    def __init__(self):
        """Initializes the training model class with logging."""
        self.log_writer = logger.App_Logger()
        self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')

    def _log_start_of_training(self):
        """Logs the start of the training process."""
        self.log_writer.log(self.file_object, 'Start of Training')

    def _log_end_of_training(self, success=True):
        """Logs the end of the training process based on success status."""
        if success:
            self.log_writer.log(self.file_object, 'Successful End of Training')
        else:
            self.log_writer.log(self.file_object, 'Unsuccessful End of Training')

    def _get_data(self):
        """Fetches data from the source."""
        data_getter = data_loader.Data_Getter(self.file_object, self.log_writer)
        return data_getter.get_data()

    def _preprocess_data(self, data):
        """Preprocesses the data by handling missing values and removing unnecessary columns."""
        preprocessor = preprocessing.Preprocessor(self.file_object, self.log_writer)
        data = preprocessor.remove_columns(data, ['Wafer'])  # remove the unnamed column
        X, Y = preprocessor.separate_label_feature(data, label_column_name='Output')

        # Handle missing values
        if preprocessor.is_null_present(X):
            X = preprocessor.impute_missing_values(X)

        # Drop columns with zero standard deviation
        cols_to_drop = preprocessor.get_columns_with_zero_std_deviation(X)
        X = preprocessor.remove_columns(X, cols_to_drop)

        return X, Y

    def _apply_clustering(self, X):
        """Applies clustering on the features using KMeans."""
        kmeans = clustering.KMeansClustering(self.file_object, self.log_writer)
        number_of_clusters = kmeans.elbow_plot(X)  # Optimal number of clusters
        X = kmeans.create_clusters(X, number_of_clusters)
        return X

    def _train_models_for_clusters(self, X, Y):
        """Trains individual models for each cluster in the data."""
        list_of_clusters = X['Cluster'].unique()
        file_op = file_methods.File_Operation(self.file_object, self.log_writer)

        for i in list_of_clusters:
            cluster_data = X[X['Cluster'] == i]
            cluster_features = cluster_data.drop(['Labels', 'Cluster'], axis=1)
            cluster_label = cluster_data['Labels']

            # Split the data into training and test sets for each cluster
            x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3, random_state=355)

            # Find the best model for the current cluster
            model_finder = tuner.Model_Finder(self.file_object, self.log_writer)
            best_model_name, best_model = model_finder.get_best_model(x_train, y_train, x_test, y_test)

            # Save the best model
            file_op.save_model(best_model, f"{best_model_name}{i}")

    def train_model(self):
        """Main method to train the model."""
        self._log_start_of_training()

        try:
            # Step 1: Get the data
            data = self._get_data()

            # Step 2: Preprocess the data
            X, Y = self._preprocess_data(data)

            # Step 3: Apply clustering
            X = self._apply_clustering(X)

            # Step 4: Train models for each cluster
            X['Labels'] = Y
            self._train_models_for_clusters(X, Y)

            # Step 5: Log the successful end of training
            self._log_end_of_training(success=True)

        except Exception as e:
            # Log error and unsuccessful end of training
            self._log_end_of_training(success=False)
            self.log_writer.log(self.file_object, f"Error during training: {e}")
            raise e

        finally:
            self.file_object.close()


# Example usage:
# model_trainer = TrainModel()
# model_trainer.train_model()
