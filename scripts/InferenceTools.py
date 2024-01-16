# Module Imports
import mariadb

import pandas as pd
import numpy as np

from datetime import datetime, timedelta, timezone
import time
import ntplib

import joblib

import os
import sys
sys.path.append("/home/Zero/Scrivania/btcpricepredictionvenv/scripts/")

import secret
import InferenceTools

ModelsDir= "/home/Zero/Scrivania/btcpricepredictionvenv/model/"

def LoadModel():
    return joblib.load(ModelsDir + "Model.pkl")

def LoadScaler():
    return joblib.load(ModelsDir + "Scaler.pkl")

def GetLatestPrediciton(cursor, connection):
    cursor.execute(f'SELECT MAX(Date) FROM {secret.MariaDB_PredictionTableName}')
    
    for x in cursor:
        getpredictionsfrom = x[0] + timedelta(hours=1)
    
    return datetime.strftime(getpredictionsfrom, "%Y-%m-%d %H:%M:%S")  #.replace(tzinfo=timezone.utc)

def GetCurrentDate():
#Times are ALWAYS UTC Aware
    try:
        client = ntplib.NTPClient()
        response = client.request('pool.ntp.org')
        todayhour = (datetime.fromtimestamp(response.tx_time, tz=timezone.utc)).replace(minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")
        return todayhour

    except:
        print("Could not sync with time server.")

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

def LoadPredictions(Preds, Dates, cursor, connection):    
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