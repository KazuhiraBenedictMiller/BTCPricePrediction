# Module Imports
import mariadb

import pandas as pd

import requests

from datetime import datetime, timedelta, timezone
import time
import ntplib

import os
import sys
sys.path.append("../scripts/")

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

def GetCurrentDatePreviousHour():
#Times are ALWAYS UTC Aware
    try:
        client = ntplib.NTPClient()
        response = client.request('pool.ntp.org')
        todayprevhour = (datetime.fromtimestamp(response.tx_time, tz=timezone.utc) - timedelta(hours=1)).replace(minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")
        return todayprevhour

    except:
        print("Could not sync with time server.")

cur, con = ConnectMariaDB()        
        
def E():
    lastdate = datetime.strptime(ETLTools.FetchLastDate(cur), "%Y-%m-%d %H:%M:%S")
    currentdate = datetime.strptime(GetCurrentDatePreviousHour(), "%Y-%m-%d %H:%M:%S")
        
    
    if lastdate < currentdate:
        RawData = ETLTools.FetchData(lastdate, currentdate)
        RawData.to_parquet("./RawData.parquet")
        
    elif lastdate == currentdate:
        RawData = ETLTools.FetchSingleRecord(cur, con, lastdate, currentdate)
        RawData.to_parquet("./RawData.parquet")
        
    else:
        print("Data already up to Date")

        print(lastdate)
        print(currentdate)
        
def T():
    RawData = pd.read_parquet("./RawData.parquet")
    CleanData = ETLTools.CleanRawData(RawData)
    CleanData.to_parquet("./CleanData.parquet")
    
def L():
    CleanData = pd.read_parquet("./CleanData.parquet")
    ETLTools.LoadDataToMariaDB(cur, con, CleanData)

def Clean():
    os.remove("./RawData.parquet")
    os.remove("./CleanData.parquet")
    
    
    
    
args = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
    }    

if __name__ == "__main__":
    E()
    T()
    L()
    Clean()
    
    
    
    
    
'''





dag = DAG(
    'Hourly_ETL_Pipeline',
    default_args = args,
    description = 'An ETL Pipeline for filling Local MariaDB',
    schedule_interval = timedelta(hours=1),
    start_date = GetCurrentTime()
    )

Extract = PythonOperator(
    task_id = 'Extract',
    python_callable = my_python_function,
    dag = dag,
)

Transform = PythonOperator(
    task_id = 'Extract',
    python_callable = my_python_function,
    dag = dag,
)

Load = PythonOperator(
    task_id = 'Extract',
    python_callable = my_python_function,
    dag = dag,
)

Extract >> Tranform >> Load
'''