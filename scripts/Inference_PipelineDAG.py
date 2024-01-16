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

#Create table if it doesn't exist
#Get Data
#Generate Training Set
#Fill Table with Predictions

def CheckAndCreateTable(cursor, connection):
    cursor.execute(f'CREATE TABLE IF NOT EXISTS {secret.MariaDB_PredictionTableName} (Date DATETIME PRIMARY KEY NOT NULL, Prediction DOUBLE)')

    
    
if __name__ == "__main__":    
    CheckAndCreateTable(cur, con)
    Model = InferenceTools.LoadModel()
    Scaler = InferenceTools.LoadScaler()
    LastestPred = InferenceTools.GetLatestPrediciton(cur, con)
    
    print(LastestPred)
    

'''
args = {
    'owner': 'admin',
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
    }    

dag = DAG(
    'Hourly_Inference_Pipeline',
    default_args = args,
    description = 'An Inference Pipeline for filling Local MariaDB with Predictions',
    schedule_interval = '@hourly',
    start_date = datetime(2024, 1, 1),
    catchup=False
    )

CreateTable = PythonOperator(
    task_id = 'Extract',
    python_callable = CheckAndCreateTable,
    op_args = [cur, con],
    dag = dag,
)

Extract >> Transform >> Load >> CleanFiles
'''