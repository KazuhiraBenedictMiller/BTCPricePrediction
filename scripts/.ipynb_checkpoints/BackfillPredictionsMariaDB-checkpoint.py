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
    cols.append("ActualDate")
    
    vals = []
    prices = list(DataDF["Close"])
    dates = list(DataDF["Date"])
    for x in range(HoursTwoWeeks+1, len(prices)+1):
        featurevals = [i for i in prices[x-HoursTwoWeeks-1:x]]
        featurevals.append(dates[x-1])
        vals.append(featurevals)
        
    FeaturesDF = pd.DataFrame(data=vals, columns=cols)
    
    return FeaturesDF

def GetPredictions(Model, Scaler, FeaturesTargets):
    Features = np.array(FeaturesTargets.drop(["ActualClose", "ActualDate"], axis=1, inplace=False))
    Targets = np.array(FeaturesTargets["ActualClose"])
    Dates = np.array(FeaturesTargets["ActualDate"])

    print(f"{Features.shape = }")
    print(f"{Targets.shape = }")
    print(f"{Date.shape = }")
    

if __name__ == "__main__":
    CheckAndCreateTable(cur, con)
    Model = LoadModel()
    Scaler = LoadScaler()

    #Fix Number of which we actually want the Predictions, then Sum x Hours to generate Features, in our case one Week
    HoursAgo = 168
    nFeatures = 336
    CleanData = GetCleanData(cur, con)
    CleanDataSlice = CleanData.loc[len(CleanData.index)-HoursAgo-nFeatures:]
    print(CleanDataSlice)
    Features = GenerateFeatures(CleanDataSlice)
    print(Features)
    Predictions = GetPredictions(Model, Scaler, Features)

    
    
#Create table if it doesn't exist
#Get Model
#Get Data
#Generate Training Set
#Fill Table with Predictions
