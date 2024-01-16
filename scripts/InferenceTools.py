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
    cursor.execute(f'SELECT MAX(Date) FROM {secret.MariaDB_PredictionsTableName}')
    
    for x in cursor:
        lastprediction = x[0]
    
    return datetime.strftime(lastprediction, "%Y-%m-%d %H:%M:%S")  #.replace(tzinfo=timezone.utc)






def GetCleanData(cursor, connection):
    cursor.execute(f'SELECT * FROM {secret.MariaDB_TableName}')

    DataDF = pd.DataFrame(data = [x for x in cursor], columns = ["Date", "Close"])
    
    return DataDF