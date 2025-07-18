import os
import pickle
import shutil

class ModelHandler:
    def __init__(self, log_file, logger):
        self.log_file = log_file
        self.logger = logger
        self.model_path = 'models/'

    def save(self, model, model_name):
        self.logger.log(self.log_file, 'Starting save process for model')
        try:
            directory = os.path.join(self.model_path, model_name)
            if os.path.exists(directory):
                shutil.rmtree(directory)
            os.makedirs(directory)

            model_file_path = os.path.join(directory, f"{model_name}.sav")
            with open(model_file_path, 'wb') as model_file:
                pickle.dump(model, model_file)
            
            self.logger.log(self.log_file, f'Model saved successfully as {model_name}.sav')
            return 'success'

        except Exception as e:
            self.logger.log(self.log_file, f'Error while saving model: {e}')
            raise

    def load(self, model_name):
        self.logger.log(self.log_file, 'Starting load process for model')
        try:
            model_file_path = os.path.join(self.model_path, model_name, f"{model_name}.sav")
            with open(model_file_path, 'rb') as model_file:
                model = pickle.load(model_file)
            
            self.logger.log(self.log_file, f'Model {model_name} loaded successfully')
            return model

        except Exception as e:
            self.logger.log(self.log_file, f'Error while loading model: {e}')
            raise

    def get_model_for_cluster(self, cluster_number):
        self.logger.log(self.log_file, 'Searching for model file based on cluster number')
        try:
            model_files = [f for f in os.listdir(self.model_path) if str(cluster_number) in f]
            if model_files:
                model_name = model_files[0].split('.')[0]
                self.logger.log(self.log_file, f'Model {model_name} found for cluster {cluster_number}')
                return model_name
            else:
                raise FileNotFoundError(f'No model found for cluster {cluster_number}')
        except Exception as e:
            self.logger.log(self.log_file, f'Error finding model: {e}')
            raise
