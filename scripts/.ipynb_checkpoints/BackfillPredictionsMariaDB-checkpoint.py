#Backfilling the Predictions Table with Actual Predicitons for the Last 10 Days

#Importing Modules
import mariadb

import pandas as pd
import numpy as np

import requests
import joblib

from datetime import datetime, timedelta, timezone
import time
import ntplib

import os
import sys
sys.path.append("/home/Zero/Scrivania/btcpricepredictionvenv/scripts/")

import secret
import InferenceTools

from sklearn.preprocessing import StandardScaler as SS
from sklearn.metrics import mean_absolute_error as MAE

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
ModelsDir= "/home/Zero/Scrivania/btcpricepredictionvenv/model/"

def CheckAndCreateTable(cursor, connection):
    cursor.execute(f'CREATE TABLE IF NOT EXISTS {secret.MariaDB_PredictionTableName} (Date DATETIME PRIMARY KEY NOT NULL, Prediction DOUBLE)')
    
def LoadModel():
    return joblib.load(ModelsDir + "Model.pkl")

def LoadScaler():
    return joblib.load(ModelsDir + "Scaler.pkl")

def GetCleanData(cursor, connection):
    cursor.execute(f'SELECT * FROM {secret.MariaDB_TableName}')

    DataDF = pd.DataFrame(data = [x for x in cursor], columns = ["Date", "Close"])
    
    return DataDF

def GenerateFeatures(DataDF):
    HoursTwoWeeks = 24*7*2
    #Generating Columns for FeaturesDF
    cols = [f'Close_{x}_Hours_Ago' for x in range(1, HoursTwoWeeks+1)]
    cols.reverse()
    cols.append("PredictionTargetDate")
    vals = []
    prices = list(DataDF["Close"])
    dates = list(DataDF["Date"])
    
    for x in range(HoursTwoWeeks, len(prices)):
        featurevals = [i for i in prices[x+1-HoursTwoWeeks:x+1]]
        featurevals.append(dates[x] + timedelta(hours=1))
        vals.append(featurevals)
    FeaturesDF = pd.DataFrame(data=vals, columns=cols)
    
    return FeaturesDF

def GetPredictions(Model, Scaler, FeaturesAndDates):
    Features = np.array(FeaturesAndDates.drop(["PredictionTargetDate"], axis=1, inplace=False))
    Dates = FeaturesAndDates["PredictionTargetDate"]

    print(f"{Features.shape = }")
    print(f"{Dates.shape = }")
    
    ScaledFeatures = Scaler.transform(Features)
        
    Preds = Model.predict(ScaledFeatures)
    
    return Preds, Dates

def BackfillLoadPredictions(Preds, Dates, cursor, connection):    
    MergedDF = pd.DataFrame(list(zip(Dates, Preds)), columns=["PredictionTargetDate", "Predictions"])

    for i in MergedDF.values:
        cursor.execute(
        f'INSERT {secret.MariaDB_PredictionTableName} VALUES (?, ?)',  
        (datetime(i[0].year, i[0].month, i[0].day, i[0].hour, i[0].minute, i[0].second), i[1]))
    
    connection.commit()
    
    #Check Inserted Data 
    cursor.execute(f'SELECT * FROM {secret.MariaDB_PredictionTableName}')

    checkdf = pd.DataFrame(data = [x for x in cursor], columns = ["PredictionTargetDate", "Prediction"])
    print(checkdf)
    
if __name__ == "__main__":
    CheckAndCreateTable(cur, con)
    Model = LoadModel()
    Scaler = LoadScaler()
    
    #Fix Number of which we actually want the Predictions, then Sum x Hours to generate Features, in our case one Week
    HoursAgo = 168
    nFeatures = 336
    CleanData = GetCleanData(cur, con)
    
    CleanDataSlice = CleanData.loc[len(CleanData.index)-HoursAgo-nFeatures-1:]
    
    #print(CleanDataSlice)
    Features = GenerateFeatures(CleanDataSlice)    
    #print(Features)
    
    Predictions, PredictionsDates = GetPredictions(Model, Scaler, Features)
    
    BackfillLoadPredictions(Predictions, PredictionsDates, cur, con)