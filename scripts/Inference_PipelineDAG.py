# Module Imports
import mariadb

import pandas as pd

import requests

from datetime import datetime, timedelta, timezone
import time
import ntplib

import joblib

import os
import sys
sys.path.append("/home/Zero/Scrivania/btcpricepredictionvenv/scripts/")

import secret
import InferenceTools

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

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
DAGTempFiles = "/home/Zero/Scrivania/btcpricepredictionvenv/tempfiles/"

def CheckAndCreateTable(cursor, connection):
    cursor.execute(f'CREATE TABLE IF NOT EXISTS {secret.MariaDB_PredictionTableName} (Date DATETIME PRIMARY KEY NOT NULL, Prediction DOUBLE)')

def GetCleanData(cursor, connection):
    LastestPred = InferenceTools.GetLatestPrediciton(cursor, connection)
        
    nFeatures = 336
    
    lastdate = datetime.strptime(InferenceTools.GetLatestPrediciton(cursor, connection), "%Y-%m-%d %H:%M:%S")
    currentdate = datetime.strptime(InferenceTools.GetCurrentDate(), "%Y-%m-%d %H:%M:%S")
    
    HoursGap = int((currentdate - lastdate).total_seconds() // 3600)
    
    CleanData = InferenceTools.GetCleanData(cursor, connection)
    CleanDataSlice = CleanData.loc[len(CleanData.index)-HoursGap-nFeatures-1:]
    
    CleanDataSlice.to_csv(DAGTempFiles + "CleanDataSlice.csv", index=False)
    
def GenerateFeaturesFromSliceData():
    CleanDataSlice = pd.read_csv(DAGTempFiles + "CleanDataSlice.csv")

    CleanDataSlice["Date"] = list(CleanDataSlice["Date"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")))

    
    Features = InferenceTools.GenerateFeatures(CleanDataSlice)
    print(Features)
    Features.to_csv(DAGTempFiles + "Features.csv", index=False)

def GeneratePredictions():
    Model = InferenceTools.LoadModel()
    Scaler = InferenceTools.LoadScaler()
    
    Features = pd.read_csv(DAGTempFiles + "Features.csv")
    
    #print(Features)
    
    Predictions, PredictionsDates = InferenceTools.GetPredictions(Model, Scaler, Features)
    
    MergedDF = pd.DataFrame(list(zip(PredictionsDates, Predictions)), columns=["PredictionTargetDate", "Predictions"])
    
    MergedDF.to_csv(DAGTempFiles + "Predictions.csv", index=False)

def LoadToMariaDB(cursor, connection):
    Predictions = pd.read_csv(DAGTempFiles + "Predictions.csv")

    Predictions["PredictionTargetDate"] = list(Predictions["PredictionTargetDate"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S")))
    
    for i in Predictions.values:
        cursor.execute(
        f'INSERT {secret.MariaDB_PredictionTableName} VALUES (?, ?)',  
        (datetime(i[0].year, i[0].month, i[0].day, i[0].hour, i[0].minute, i[0].second), i[1]))
    
    connection.commit()
    
    #Check Inserted Data 
    cursor.execute(f'SELECT * FROM {secret.MariaDB_PredictionTableName}')

    checkdf = pd.DataFrame(data = [x for x in cursor], columns = ["PredictionTargetDate", "Prediction"])
    print(checkdf)
    
def Clean():
    os.remove(DAGTempFiles + "CleanDataSlice.csv")    
    os.remove(DAGTempFiles + "Features.csv")
    os.remove(DAGTempFiles + "Predictions.csv")

args = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
    }    

dag = DAG(
    'Hourly_Inference_Pipeline',
    default_args = args,
    description = 'An Inference Pipeline for filling Local MariaDB with Predictions',
    schedule_interval = '3 * * * *',#'@hourly',
    start_date = datetime(2024, 1, 1, 0, 3, 0),
    catchup=False
    )

CreateTable = PythonOperator(
    task_id = 'CheckAndCreateTable',
    python_callable = CheckAndCreateTable,
    op_args = [cur, con],
    dag = dag,
)

GetData = PythonOperator(
    task_id = 'GetCleanData',
    python_callable = GetCleanData,
    op_args = [cur, con],
    dag = dag,
)

GenerateFeatures = PythonOperator(
    task_id = 'GenerateFeatures',
    python_callable = GenerateFeaturesFromSliceData,
    dag = dag,
)

Inference = PythonOperator(
    task_id = 'GeneratePredictions',
    python_callable = GeneratePredictions,
    dag = dag,
)

LoadPredictions = PythonOperator(
    task_id = 'LoadPredictionsToDB',
    python_callable = LoadToMariaDB,
    op_args = [cur, con],
    dag = dag,
)

CreateTable >> GetData >> GenerateFeatures >> Inference >> LoadPredictions 

'''
CleanFiles = PythonOperator(
    task_id = 'CleanFiles',
    python_callable = Clean,
    dag = dag,
)

>> CleanFiles

if __name__ == "__main__":    
    CheckAndCreateTable(cur, con)
    GetCleanData(cur, con)
    GenerateFeaturesFromSliceData()
    GeneratePredictions()
    
    lastdate = datetime.strptime(InferenceTools.GetLatestPrediciton(cur, con), "%Y-%m-%d %H:%M:%S")
    currentdate = datetime.strptime(InferenceTools.GetCurrentDate(), "%Y-%m-%d %H:%M:%S")
    print(lastdate)
    print(currentdate)
    
'''

    
    
