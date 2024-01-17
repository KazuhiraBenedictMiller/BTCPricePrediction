# Module Imports
import mariadb

import pandas as pd
import numpy as np

import requests

from datetime import datetime, timedelta, timezone
import time
import ntplib

import lightgbm as lgb
from sklearn.model_selection import train_test_split as TTS, TimeSeriesSplit as TSS
from sklearn.preprocessing import StandardScaler as SS
from sklearn.metrics import mean_absolute_error as MAE

import optuna

import joblib

import shutil
import os
import sys
sys.path.append("/home/Zero/Scrivania/btcpricepredictionvenv/scripts/")

import secret
import ReTrainingTools

def GetCurrentTime():
#Times are ALWAYS UTC Aware
    try:
        client = ntplib.NTPClient()
        response = client.request('pool.ntp.org')
        CurrentHour = datetime.fromtimestamp(response.tx_time)
        return CurrentHour

    except:
        print("Could not sync with time server.")
        
def ConnectMariaDB():
    # Connect to MariaDB Platform
    try:
        conn = mariadb.connect(
            user=secret.MariaDB_User,
            password=secret.MariaDB_Password,
            host="127.0.0.1",
            port=3306,
            database=secret.MariaDB_Database
        )
        
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        sys.exit(1)

    # Get Cursor
    cur = conn.cursor()
    
    return cur, conn

cur, con = ConnectMariaDB()

ModelsDir = "/home/Zero/Scrivania/btcpricepredictionvenv/model/"
ModelStore = "/home/Zero/Scrivania/btcpricepredictionvenv/modelstore/"
DAGTempFiles = "/home/Zero/Scrivania/btcpricepredictionvenv/tempfiles/"

#Train new Model, push it to Store, check all the Models and pick the best one -> move the best to ModelsDir

ModelPrefix = "ModelVersion"
_, _, files = next(os.walk(ModelStore))
file_count = len(files) // 2

ModelVersion = file_count

def GetTrainingData(cursor, connection):
    DataDF = ReTrainingTools.GetCleanData(cursor, connection)
    
    DataDF.to_csv(DAGTempFiles + "TrainingData.csv", index=False)

def FeaturesGeneration():
    DataDF = pd.read_csv(DAGTempFiles + "TrainingData.csv")

    Features = ReTrainingTools.GenerateFeaturesAndTargets(DataDF)
    
    Features.to_csv(DAGTempFiles + "TrainingFeatures.csv", index=False)
    
def ScalerGeneration():
    F = pd.read_csv(DAGTempFiles + "TrainingFeatures.csv")
    
    xTrain, yTrain, xTest, yTest = ReTrainingTools.SplitFeatures(F)
    
    Scaler = ReTrainingTools.ScaleFeatures(xTrain)
    
    joblib.dump(Scaler, ModelStore + "ScalerVersion" + str(ModelVersion) + ".pkl")

def Objective(T:optuna.trial.Trial) -> float:
    
    Hyperparams = {"metric":"mae",
                   "verbose":-1,
                   "num_leaves":T.suggest_int("num_leaves", 2, 256),
                   "feature_fraction":T.suggest_float("feature_fraction", 0.2, 1.0),
                   "bagging_fraction":T.suggest_float("bagging_fraction", 0.2, 1.0),
                   "min_child_samples":T.suggest_int("min_child_samples", 3, 100),
                  }
    
    tss = TSS(n_splits=2)
    Scores = []
    
    F = pd.read_csv(DAGTempFiles + "TrainingFeatures.csv")
    
    xTrain, yTrain, xTest, yTest = ReTrainingTools.SplitFeatures(F)
    
    Scaler = joblib.load(ModelStore + "ScalerVersion" + str(ModelVersion) + ".pkl")
    
    ScaledxTrain = ReTrainingTools.ScalexTrain(Scaler, xTrain)
    
    for trainIndex, valIndex in tss.split(ScaledxTrain):
        
        #Split Data for Training and Validation
        xTrain_, xVal_ = ScaledxTrain[trainIndex, :], ScaledxTrain[valIndex, :]
        yTrain_, yVal_ = yTrain[trainIndex], yTrain[valIndex]
        
        #Train the Model
        LGB = lgb.LGBMRegressor(**Hyperparams)
        LGB.fit(xTrain_, yTrain_)
        
        #Evaluate the Model
        yPred = LGB.predict(xVal_)
        mae = MAE(yVal_, yPred)
        
        Scores.append(mae)
        
    #Return Avg Score
    return np.array(Scores).mean()
    
def OptunaStudy():
    F = pd.read_csv(DAGTempFiles + "TrainingFeatures.csv")
    
    xTrain, yTrain, xTest, yTest = ReTrainingTools.SplitFeatures(F)
    
    Scaler = joblib.load(ModelStore + "ScalerVersion" + str(ModelVersion) + ".pkl")
    
    ScaledxTrain = ReTrainingTools.ScalexTrain(Scaler, xTrain)
    
    Study = optuna.create_study(direction="minimize")
    Study.optimize(Objective, n_trials=5)
    BestParams = Study.best_trial.params
    print(BestParams)
    
    BestModel = ReTrainingTools.CreateAndFitModel(BestParams, ScaledxTrain, yTrain)
    
    Preds = BestModel.predict(Scaler.transform(xTest))
    testMae = MAE(yTest, Preds)
    print(f"{testMae = :.4f}")
    
    joblib.dump(BestModel, ModelStore + "ModelVersion" + str(ModelVersion) + ".pkl")
    
def FindPerformingModel():
    
    F = pd.read_csv(DAGTempFiles + "TrainingFeatures.csv")
    
    xTrain, yTrain, xTest, yTest = ReTrainingTools.SplitFeatures(F)
    
    ReTrainingTools.FindBestModel(ModelStore, xTest, yTest)
    
def Clean():
    os.remove(DAGTempFiles + "TrainingData.csv")    
    os.remove(DAGTempFiles + "TrainingFeatures.csv")
    
    
    
    
if __name__ == "__main__":
    GetTrainingData(cur, con)
    FeaturesGeneration()
    ScalerGeneration()
    OptunaStudy()
    FindPerformingModel()
    

'''
args = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
    }    

dag = DAG(
    'Weekly_ReTraining_Pipeline',
    default_args = args,
    description = 'A Model ReTraining Pipeline that Every Week Generates a new model and Finds the Best One',
    schedule_interval = '30 * * * *', #30 23 * * 0 #'@hourly',
    start_date = datetime(2024, 1, 1, 0, 30, 0),
    catchup=False
    )

GetData = PythonOperator(
    task_id = 'GetTrainingData',
    python_callable = GetTrainingData,
    op_args = [cur, con],
    dag = dag,
)

GenerateFeatures = PythonOperator(
    task_id = 'FeaturesGeneration',
    python_callable = FeaturesGeneration,
    dag = dag,
)

GenerateScaler = PythonOperator(
    task_id = 'ScalerGeneration',
    python_callable = ScalerGeneration,
    dag = dag,
)

Study = PythonOperator(
    task_id = 'OptunaStudy',
    python_callable = OptunaStudy,
    dag = dag,
)

FindBestModel = PythonOperator(
    task_id = 'FindPerformingModel',
    python_callable = FindPerformingModel,
    dag = dag,
)

CleanFiles = PythonOperator(
    task_id = 'CleanFiles',
    python_callable = Clean,
    dag = dag,
)

GetData >> FeaturesGeneration >> GenerateScaler >> Study >> FindBestModel >> CleanFiles
'''