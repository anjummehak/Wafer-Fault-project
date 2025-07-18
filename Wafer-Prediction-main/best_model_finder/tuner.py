from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from xgboost import XGBClassifier
from sklearn.metrics import roc_auc_score, accuracy_score

class ModelFinder:
    """
    This class finds the best model with optimal parameters.
    """

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        self.clf = RandomForestClassifier()
        self.xgb = XGBClassifier(objective='binary:logistic')

    def get_best_params(self, model, param_grid, train_x, train_y):
        """
        Generic method for hyperparameter tuning using GridSearchCV.
        """
        try:
            grid = GridSearchCV(model, param_grid, cv=5, verbose=3)
            grid.fit(train_x, train_y)
            return grid.best_estimator_
        except Exception as e:
            self.logger_object.log(self.file_object, f'Hyperparameter tuning failed: {str(e)}')
            raise

    def get_best_model(self, train_x, train_y, test_x, test_y):
        """
        Determines the best model based on AUC or accuracy.
        """
        try:
            param_grid_rf = {
                "n_estimators": [10, 50, 100, 130],
                "criterion": ['gini', 'entropy'],
                "max_depth": range(2, 4, 1),
                "max_features": ['auto', 'log2']
            }
            param_grid_xgb = {
                'learning_rate': [0.5, 0.1, 0.01, 0.001],
                'max_depth': [3, 5, 10, 20],
                'n_estimators': [10, 50, 100, 200]
            }
            
            best_rf = self.get_best_params(self.clf, param_grid_rf, train_x, train_y)
            best_xgb = self.get_best_params(self.xgb, param_grid_xgb, train_x, train_y)
            
            rf_pred = best_rf.predict(test_x)
            xgb_pred = best_xgb.predict(test_x)
            
            if len(test_y.unique()) == 1:
                rf_score = accuracy_score(test_y, rf_pred)
                xgb_score = accuracy_score(test_y, xgb_pred)
            else:
                rf_score = roc_auc_score(test_y, rf_pred)
                xgb_score = roc_auc_score(test_y, xgb_pred)
            
            self.logger_object.log(self.file_object, f'RandomForest Score: {rf_score}, XGBoost Score: {xgb_score}')
            
            return ('XGBoost', best_xgb) if xgb_score > rf_score else ('RandomForest', best_rf)
        except Exception as e:
            self.logger_object.log(self.file_object, f'Model selection failed: {str(e)}')
            raise
