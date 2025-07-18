import os
from flask import Flask, request, render_template, Response
from flask_cors import CORS, cross_origin
from wsgiref import simple_server
import json

# Importing necessary modules
from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
from predictFromModel import prediction
import flask_monitoringdashboard as dashboard

# Set environment variables
os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

# Initialize Flask app and configure dashboard
app = Flask(__name__)
dashboard.bind(app)
CORS(app)

# Helper function for response
def create_response(message):
    return Response(message)

# Helper function for handling predictions
def handle_prediction_request(path):
    try:
        pred_val = pred_validation(path)
        pred_val.prediction_validation()
        
        pred = prediction(path)
        path, json_predictions = pred.predictionFromModel()
        
        return create_response(f"Prediction File created at {path} and few of the predictions are {json.loads(json_predictions)}")
    except Exception as e:
        return create_response(f"Error Occurred: {str(e)}")

# Helper function for handling training request
def handle_training_request(path):
    try:
        train_valObj = train_validation(path)
        train_valObj.train_validation()

        trainModelObj = trainModel()
        trainModelObj.trainingModel()
        
        return create_response("Training successful!")
    except Exception as e:
        return create_response(f"Error Occurred: {str(e)}")

@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route("/predict", methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        data = request.json if request.json else request.form
        if 'filepath' in data:
            path = data['filepath']
            return handle_prediction_request(path)
        else:
            return create_response("Filepath not found in the request")
    except Exception as e:
        return create_response(f"Error Occurred: {str(e)}")

@app.route("/train", methods=['POST'])
@cross_origin()
def trainRouteClient():
    try:
        data = request.json
        if 'folderPath' in data:
            path = data['folderPath']
            return handle_training_request(path)
        else:
            return create_response("Folder path not found in the request")
    except Exception as e:
        return create_response(f"Error Occurred: {str(e)}")

# Running the Flask app
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    host = '0.0.0.0'
    httpd = simple_server.make_server(host, port, app)
    httpd.serve_forever()
