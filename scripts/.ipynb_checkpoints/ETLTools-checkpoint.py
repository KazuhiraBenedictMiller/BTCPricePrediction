# Module Imports
import mariadb

import pandas as pd

import requests

from datetime import datetime, timedelta, timezone
import time
import ntplib

import sys
sys.path.append("/home/Zero/Scrivania/btcpricepredictionvenv/scripts/")

import secret

def GetCurrentDatePreviousHour():
#Times are ALWAYS UTC Aware
    try:
        client = ntplib.NTPClient()
        response = client.request('pool.ntp.org')
        todayprevhour = (datetime.fromtimestamp(response.tx_time, tz=timezone.utc) - timedelta(hours=1)).replace(minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")
        return todayprevhour

    except:
        print("Could not sync with time server.")

def FetchLastDate(cursor):
#Fetching the Last Date from DB and adding an hour as the hour to Start Fetching From
    cursor.execute(f'SELECT MAX(Date) FROM {secret.MariaDB_TableName}')
    
    for x in cursor:
        startfetchfrom = x[0] + timedelta(hours=1)
    
    return datetime.strftime(startfetchfrom, "%Y-%m-%d %H:%M:%S")  #.replace(tzinfo=timezone.utc)

#Extract
def FetchData(start, end):
    product_id = "BTC-USD"
    startdate = start
    enddate = end
    
    RawTempList = []
    
    while startdate < enddate:    
        tempfetchenddate = startdate + timedelta(hours=299) #Let's Keep a Reserve of 2 Candles since it goes from start to end included

        if tempfetchenddate >= enddate:
            tempfetchenddate = enddate

        #Fetch

        URL = f'https://api.exchange.coinbase.com/products/{product_id}/candles?start={startdate}&end={tempfetchenddate}&granularity=3600'
        r = requests.get(URL)
        data = r.json()

        RawTempList.extend(data)

        startdate = tempfetchenddate  
    
    RawData = pd.DataFrame(RawTempList, columns = ["Date", "Open", "High", "Low", "Close", "Volume"])
    RawData["Date"] = RawData["Date"].apply(lambda x: datetime.fromtimestamp(x, tz= timezone.utc))
    RawData = RawData.sort_values(by=["Date"])

    return RawData

#Check and Clean Duplicates
def CleanDuplicates(DF):
    Duplicates = DF.loc[DF.duplicated() == True]
    
    if not Duplicates.empty:
        DF.drop_duplicates(keep="first", inplace = True)

    return DF

#Check and Fill Missing Values
def CheckFillMissing(DF):
    
    Dates = [datetime.strptime(x[:-6], "%Y-%m-%d %H:%M:%S") for x in list(DF["Date"])] #list(DF["Date"])
    Missing = []

    testdate = Dates[0]

    while testdate <= Dates[-1]:
        if testdate not in Dates:
            Missing.append(testdate)

        testdate += timedelta(hours=1)
     
    if not Missing:
        templist = []
        
        for x in Missing:
            RecordToCopy = DF[DF["Date"] == Missing[0] - timedelta(hours=1)]
            
            hoursago = 2
            while not RecordToCopy:
                RecordToCopy = DF[DF["Date"] == Missing[0] - timedelta(hours=hoursago)]
                hoursago += 1
                
            templist.extend([x, RecordToCopy.iloc[0]["Close"], RecordToCopy.iloc[0]["Close"], RecordToCopy.iloc[0]["Close"], 
                             RecordToCopy.iloc[0]["Close"],RecordToCopy.iloc[0]["Volume"]])
            
        tempdf = pd.DataFrame(templist, columns=["Date", "Open", "High", "Low", "Close", "Volume"])

        DF = pd.concat([DF, tempdf])
        
    return DF

#Transform
def CleanRawData(RawDF):
    
    NoDuplicatesDF = CleanDuplicates(RawDF)
    NoMissingDF = CheckFillMissing(NoDuplicatesDF)
    
    CleanData = NoMissingDF.sort_values(by="Date")
    
    #Drop Useless Columns
    
    CleanData.drop(["Open", "High", "Low", "Volume"], axis=1, inplace=True)
    
    return CleanData

#Load
def LoadDataToMariaDB(cursor, connection, DataDF):
    for i in DataDF.values:
        Date = datetime.strptime(i[0][:-6], "%Y-%m-%d %H:%M:%S")
        cursor.execute(
        f'INSERT {secret.MariaDB_TableName} VALUES (?, ?)', 
        (datetime(Date.year, Date.month, Date.day, Date.hour, Date.minute, Date.second), i[1]))
    
    connection.commit()
    
    #Check Inserted Data 
    cursor.execute(f'SELECT * FROM {secret.MariaDB_TableName}')

    checkdf = pd.DataFrame(data = [x for x in cursor], columns = ["Date", "Close"])
    print(checkdf)

#Clean Single Record
def CleanSingleRecord(RawDF):
    #Drop Useless Columns
    
    RawDF.drop(["Open", "High", "Low", "Volume"], axis=1, inplace=True)
    
    return RawDF

#Fetch Single Record
def FetchSingleRecord(cursor, connection, startdate, enddate):
    product_id = "BTC-USD"
    
    RawTempList = []

    #Fetch

    URL = f'https://api.exchange.coinbase.com/products/{product_id}/candles?start={startdate}&end={enddate}&granularity=3600'
    r = requests.get(URL)
    data = r.json()

    RawTempList.extend(data)
    
    RawData = pd.DataFrame(RawTempList, columns = ["Date", "Open", "High", "Low", "Close", "Volume"])
    RawData["Date"] = RawData["Date"].apply(lambda x: datetime.fromtimestamp(x, tz= timezone.utc))
    RawData = RawData.sort_values(by=["Date"])

    return RawData