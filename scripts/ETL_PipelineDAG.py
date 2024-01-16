# Module Imports
import mariadb

import pandas as pd

import requests

from datetime import datetime, timedelta, timezone
import time
import ntplib

import os
import sys
sys.path.append("/home/Zero/Scrivania/btcpricepredictionvenv/scripts/")

import secret
import ETLTools

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
DAGTempFiles = "/home/Zero/Scrivania/btcpricepredictionvenv/tempfiles/"
        
def E():
    lastdate = datetime.strptime(ETLTools.FetchLastDate(cur), "%Y-%m-%d %H:%M:%S")
    currentdate = datetime.strptime(ETLTools.GetCurrentDatePreviousHour(), "%Y-%m-%d %H:%M:%S")
    
    if lastdate < currentdate:
        RawData = ETLTools.FetchData(lastdate, currentdate)
        #RawData.to_parquet(DAGTempFiles + "RawData.parquet")
        RawData.to_csv(DAGTempFiles + "RawData.csv", index=False)
        
    elif lastdate == currentdate:
        RawData = ETLTools.FetchSingleRecord(cur, con, lastdate, currentdate)
        #RawData.to_parquet(DAGTempFiles + "RawData.parquet")
        RawData.to_csv(DAGTempFiles + "RawData.csv", index=False)
        
    else:
        print("Data already up to Date")

        print(lastdate)
        print(currentdate)
        
def T():
    #RawData = pd.read_parquet(DAGTempFiles + "RawData.parquet")
    RawData = pd.read_csv(DAGTempFiles + "RawData.csv")
    
    if len(RawData.index) > 1:
        CleanData = ETLTools.CleanRawData(RawData)
        #CleanData.to_parquet(DAGTempFiles + "CleanData.parquet")
        CleanData.to_csv(DAGTempFiles + "CleanData.csv", index=False)
        
    elif len(RawData.index) == 1:
        CleanData = ETLTools.CleanSingleRecord(RawData)
        #CleanData.to_parquet(DAGTempFiles + "CleanData.parquet")
        CleanData.to_csv(DAGTempFiles + "CleanData.csv", index=False)
        
def L(cursor, connection):
    #CleanData = pd.read_parquet(DAGTempFiles + "CleanData.parquet")
    CleanData = pd.read_csv(DAGTempFiles + "CleanData.csv")
    ETLTools.LoadDataToMariaDB(cursor, connection, CleanData)

def Clean():
    os.remove(DAGTempFiles + "RawData.parquet")
    os.remove(DAGTempFiles + "CleanData.parquet")

args = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
    }    

dag = DAG(
    'Hourly_ETL_Pipeline',
    default_args = args,
    description = 'An ETL Pipeline for filling Local MariaDB',
    schedule_interval = '@hourly',
    start_date = datetime(2024, 1, 1),
    catchup=False
    )

Extract = PythonOperator(
    task_id = 'Extract',
    python_callable = E,
    dag = dag,
)

Transform = PythonOperator(
    task_id = 'Transform',
    python_callable = T,
    dag = dag,
)

Load = PythonOperator(
    task_id = 'Load',
    python_callable = L,
    op_args = [cur, con],
    dag = dag,
)

CleanFiles = PythonOperator(
    task_id = 'CleanFiles',
    python_callable = Clean,
    dag = dag,
)

Extract >> Transform >> Load >> CleanFiles