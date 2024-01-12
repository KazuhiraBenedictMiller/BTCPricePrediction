# Module Imports
import mariadb

import pandas as pd

import requests

from datetime import datetime, timedelta
import time
import ntplib

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

args = {
    'owner': 'admin',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
    }        

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





        newdata = CleanRawData(data)
        LoadDataToMariaDB(cur, con, newdata)

    elif lastdate == currentdate:
        FetchAndLoadSingleRecord(cur, con, lastdate, currentdate)








def E():
    cur, con = ConnectMariaDB()

    lastdate = datetime.strptime(ETLTools.FetchLastDate(cur), "%Y-%m-%d %H:%M:%S")
    currentdate = datetime.strptime(ETLTools.GetCurrentDatePreviousHour(), "%Y-%m-%d %H:%M:%S")
    
    if lastdate < currentdate:
        RawData = ETLTools.FetchData()
        RawData.to_parquet("./RawData.parquet")
    
    
    
    

    
E()
    
    
    
    
    
    
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