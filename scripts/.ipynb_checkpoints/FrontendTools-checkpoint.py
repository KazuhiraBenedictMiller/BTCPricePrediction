import mariadb

import pandas as pd
import numpy as np

from datetime import datetime, timedelta, timezone
import time
import ntplib

from sklearn.metrics import mean_absolute_error as MAE

import streamlit as st
from bokeh.plotting import figure, show

import os
import sys
#sys.path.append("/home/Zero/Scrivania/btcpricepredictionvenv/scripts/")
sys.path.append("../scripts/")

import secret

st.set_page_config(layout = "wide")

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

def FetchPredictions(cursor, connection):
    cursor.execute(f'SELECT * FROM {secret.MariaDB_PredictionTableName}')

    Preds = pd.DataFrame(data = [x for x in cursor], columns = ["PredictionTargetDate", "Prediction"])
    
    return Preds

def FetchData(cursor, connection, start):
    cursor.execute(f'SELECT * FROM {secret.MariaDB_TableName} WHERE Date>=?', (start, ))

    Closes = pd.DataFrame(data = [x for x in cursor], columns = ["Date", "Close"])
    
    return Closes

if __name__ == "__main__":
    P = FetchPredictions(cur, con)
    print(P)
    
    s = P["PredictionTargetDate"][0]
    StartDate = datetime(s.year, s.month, s.day, s.hour, s.minute, s.second)
    
    print(StartDate)
    
    D = FetchData(cur, con, StartDate)
    print(D)
    
    PredsNoLast = P.drop(list(P.index)[-1], axis=0, inplace = False)
    MAEs = [abs(PredsNoLast["Prediction"][x] - D["Close"][x]) for x in range(len(PredsNoLast["Prediction"]))]
    
    #print(MAEs)
    
    ActualsAndPreds = pd.DataFrame(columns=["Date", "Actual", "Pred"])
    ActualsAndPreds["Date"] = PredsNoLast["PredictionTargetDate"]
    ActualsAndPreds["Actual"] = D["Close"]
    ActualsAndPreds["Pred"] = PredsNoLast["Prediction"]
    
    print(ActualsAndPreds)
    
        #MAE per Hour
    HourlyMAE = (
        ActualsAndPreds
        .apply(lambda df: MAE(df["Actual"], df["Pred"]))
        .reset_index()
        .rename(columns={0: "MAE"})
    )
    print(HourlyMAE)