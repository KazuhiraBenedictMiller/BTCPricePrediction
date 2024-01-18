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
sys.path.append("../scripts/")

import secret
import FrontendTools

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

#st.set_page_config(layout = "wide")

#Title
currentdate = pd.to_datetime(datetime.utcnow()).floor("H")
st.title(f"BTCUSD Price Predictions and Model Monitoring Dashboard")
st.header(f"{currentdate} UTC")

#Plotting a Progress Bar to improve UI while Loading Time
ProgressBar = st.sidebar.header("Working Progress")
ProgressBar = st.sidebar.progress(0)
N_Steps = 6

def LoadPredictions(cursor, connection):
    return FrontendTools.FetchPredictions(cursor, connection)

def LoadPartialActuals(cursor, connection, startdate):
    return FrontendTools.FetchPartialData(cursor, connection, startdate) 

def LoadFullActuals(cursor, connection):
    return FrontendTools.FetchFullData(cursor, connection) 

with st.spinner("Fetching Model Predictions and Actual Values from MariaDB"):
        
    Preds = LoadPredictions(cur, con)
    
    PredsNoLast = Preds.drop(list(Preds.index)[-1], axis=0, inplace = False)
    
    PredsStart = Preds["PredictionTargetDate"][0]
    StartDate = datetime(PredsStart.year, PredsStart.month, PredsStart.day, PredsStart.hour, PredsStart.minute, PredsStart.second)
    
    PartialActuals = LoadPartialActuals(cur, con, StartDate)
    
    FullActuals = LoadFullActuals(cur, con)
    
    ActualsAndPreds = pd.DataFrame(columns=["Date", "Actual", "Pred"])
    ActualsAndPreds["Date"] = PredsNoLast["PredictionTargetDate"]
    ActualsAndPreds["Hour"] = [x.hour for x in ActualsAndPreds["Date"]]
    ActualsAndPreds["Actual"] = PartialActuals["Close"]
    ActualsAndPreds["Pred"] = PredsNoLast["Prediction"]
    
    st.sidebar.write("Model Predictions and Actual Values Arrived")
    ProgressBar.progress(1/N_Steps)
    
with st.spinner("Plotting BTCUSD Hourly Chart (UTC-Aware Dates) on Candle Close"):
    
    st.header("BTCUSD Close Prices Chart")
    
    BTCUSD_Chart = figure(title="BTC-USD Price Chart", x_axis_label='Date', x_axis_type="datetime", y_axis_label='Hourly Close Price', width=1300)
    BTCUSD_Chart.line(FullActuals["Date"], FullActuals["Close"], legend_label="BTC-USD", line_width=2, color="black")

    st.bokeh_chart(BTCUSD_Chart, use_container_width=True)
    
    st.sidebar.write("BTCUSD Price Chart Plotted")
    ProgressBar.progress(3/N_Steps)
    
with st.spinner("Plotting BTCUSD Hourly Chart Versus Model Predictions (UTC-Aware Dates) on Candle Close"):
    st.header("BTCUSD Close Prices Versus Model Prediction Chart")

    BTCUSDPreds_Chart = figure(title="BTC-USD Price VS Predictions Chart", x_axis_label='Date', x_axis_type="datetime", y_axis_label='Hourly Close Prices VS Predictions', width=1300)
    BTCUSDPreds_Chart.line(PartialActuals["Date"], PartialActuals["Close"], legend_label="BTC-USD", line_width=2, color="black")
    BTCUSDPreds_Chart.line(PredsNoLast["PredictionTargetDate"], PredsNoLast["Prediction"], legend_label="Predictions", line_width=2, color="red")
    BTCUSDPreds_Chart.circle(Preds["PredictionTargetDate"].iloc[-1], Preds["Prediction"].iloc[-1], size=10, color="red")
    
    st.bokeh_chart(BTCUSDPreds_Chart, use_container_width=True)
    
    st.sidebar.write("BTCUSD Price Chart vs Model Preditions Plotted")
    ProgressBar.progress(4/N_Steps)
    
with st.spinner("Plotting Hourly MAE"):
    
    st.header("Mean Absolute Error (MAE) Hour-by-Hour")
        
    #Hourly MAE
    HourlyMAE = ActualsAndPreds.drop("Hour", axis=1, inplace=False).groupby("Date").apply(lambda x: MAE(x["Actual"], x["Pred"])).reset_index().rename(columns={0: "MAE"}).sort_values(by="Date")    
    
    HourlyMAE_Chart = figure(title="BTC-USD Hourly MAE Chart", x_axis_label='Date', x_axis_type="datetime", y_axis_label='MAE', width=1300)
    
    HourlyMAE_Chart.line(HourlyMAE["Date"], HourlyMAE["MAE"], legend_label="Hourly MAE", line_width=2, color="blue")

    st.bokeh_chart(HourlyMAE_Chart, use_container_width=True)

    st.sidebar.write("Hourly MAE Plotted")
    ProgressBar.progress(5/N_Steps)
    
with st.spinner("Plotting Mean Absolute Error (MAE) Hourly Aggregate"):
    
    st.header('Mean Absolute Error (MAE) Hourly Aggregate')

    #Aggregate MAE per Hour
    AggregateMAE = ActualsAndPreds.drop("Date", axis=1, inplace=False).groupby("Hour").apply(lambda x: MAE(x["Actual"], x["Pred"])).reset_index().rename(columns={0: "MAE"}).sort_values(by="Hour")
    
    AggregateMAE_Chart = figure(title="BTC-USD Aggreate Hourly MAE Chart", x_axis_label='Hour', y_axis_label='MAE', width=1300)
    
    AggregateMAE_Chart.vbar(AggregateMAE["Hour"], AggregateMAE["MAE"], legend_label="Aggregate Hourly MAE", width=0.5, fill_color="blue")

    st.bokeh_chart(AggregateMAE_Chart, use_container_width=True)
    
    st.sidebar.write("Aggregate MAE Plotted")
    ProgressBar.progress(6/N_Steps)