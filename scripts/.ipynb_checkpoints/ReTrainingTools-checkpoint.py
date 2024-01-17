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

#Train new Model, push it to Store, check all the Models and pick the best one -> move the best to ModelsDir

ModelPrefix = "ModelVersion"
_, _, files = next(os.walk(ModelStore))
file_count = len(files) // 2

ModelVersion = file_count

def GetCleanData(cursor, connection):
    cursor.execute(f'SELECT * FROM {secret.MariaDB_TableName}')

    DataDF = pd.DataFrame(data = [x for x in cursor], columns = ["Date", "Close"])
    
    return DataDF

def GenerateFeaturesAndTargets(DataDF):
    HoursTwoWeeks = 24*7*2
    #Generating Columns for FeaturesDF
    cols = [f'Close_{x}_Hours_Ago' for x in range(1, HoursTwoWeeks+1)]
    cols.reverse()
    cols.append("ActualClose")
    cols.append("ActualDate")
    vals = []
    prices = list(DataDF["Close"])
    dates = list(DataDF["Date"])
    
    for x in range(HoursTwoWeeks, len(prices)):
        featurevals = [i for i in prices[x-HoursTwoWeeks:x+1]]
        featurevals.append(dates[x])
        vals.append(featurevals)
        
    FeaturesDF = pd.DataFrame(data=vals, columns=cols)
    
    return FeaturesDF

def SplitFeatures(FeaturesAndTargets):
    Features = np.array(FeaturesAndTargets.drop(["ActualClose", "ActualDate"], axis=1, inplace=False))
    Targets = np.array(FeaturesAndTargets["ActualClose"])
    
    xTrain, xTest, yTrain, yTest = TTS(Features, Targets, test_size=0.2)
    
    return xTrain, yTrain, xTest, yTest

def ScaleFeatures(xTrain):
    Scaler = SS()

    ScaledxTrain = Scaler.fit_transform(xTrain)
    
    return Scaler

def ScalexTrain(Scaler, xTrain):
    ScaledxTrain = Scaler.transform(xTrain)
    
    return ScaledxTrain

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
    
    Scaler = joblib.load(ModelStore + "ScalerVersion" + str(ModelVersion) + ".pkl")
    ScaledxTrain = ScalexTrain(Scaler, xTrain)
    
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

def CreateAndFitModel(Best, ScaledxTrain, yTrain):
    Model = lgb.LGBMRegressor(**Best)
    
    Model.fit(ScaledxTrain, yTrain)
    
    return Model

def FindBestModel(Path, xTest, yTest):
    ModelNames = []
    
    for x in os.listdir(Path):
        if x.endswith(".pkl") and "Model" in x:
            ModelNames.append(x)
    
    ModelMAEs = pd.DataFrame(columns=["ModelVersion", "MAE"])
    
    print(ModelNames)
    
    for v in ModelNames:
        Version = v[-5]
        
        ModelPath = Path + "ModelVersion" + Version + ".pkl"
        ScalerPath = Path + "ScalerVersion" + Version + ".pkl"
        
        Model = joblib.load(ModelPath)
        Scaler = joblib.load(ScalerPath)
        
        Preds = Model.predict(Scaler.transform(xTest))
        testMae = MAE(yTest, Preds)
        print(f"{testMae = :.4f}")
        
        ModelMAEs.loc[len(ModelMAEs)] = [Version,testMae]
        
        print(Model)
    
    print(ModelMAEs)
    BestModel = ModelMAEs[ModelMAEs["MAE"] == min(list(ModelMAEs["MAE"]))]
    BestModelVersion = BestModel["ModelVersion"].values[0]
    print(BestModelVersion)
    
    #Copying Model and Scaler into Production
    #shutil.copy(src=Path + "ModelVersion" + BestModelVersion + ".pkl", dst=ModelsDir + "BestModel_ModelVersion" + BestModelVersion + ".pkl")
    #shutil.copy(src=Path + "ScalerVersion" + BestModelVersion + ".pkl", dst=ModelsDir + "BestModel_ScalerVersion" + BestModelVersion + ".pkl")
    
    os.remove(ModelsDir + "Model.pkl")
    os.remove(ModelsDir + "Scaler.pkl")    
    
    shutil.copy(src=Path + "ModelVersion" + BestModelVersion + ".pkl", dst=ModelsDir + "Model.pkl")
    shutil.copy(src=Path + "ScalerVersion" + BestModelVersion + ".pkl", dst=ModelsDir + "Scaler.pkl")

if __name__ == "__main__":
    
    data = GetCleanData(cur, con)
    print(data)
    
    f = GenerateFeaturesAndTargets(data)
    
    print(f)
    
    xTrain, yTrain, xTest, yTest = SplitFeatures(f)
    
    Scaler = ScaleFeatures(xTrain)
    print(ModelVersion)
    joblib.dump(Scaler, ModelStore + "ScalerVersion" + str(ModelVersion) + ".pkl")
    
    Study = optuna.create_study(direction="minimize")
    Study.optimize(Objective, n_trials=5)
    BestParams = Study.best_trial.params
    print(BestParams)
    
    ScaledxTrain = ScalexTrain(Scaler, xTrain)
    BestModel = CreateAndFitModel(BestParams, ScaledxTrain, yTrain)
    
    joblib.dump(BestModel, ModelStore + "ModelVersion" + str(ModelVersion) + ".pkl")
    
    FindBestModel(ModelStore, xTest, yTest)