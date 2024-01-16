'''
if __name__ == "__main__":
    lastdate = datetime.strptime(ETLTools.FetchLastDate(cur), "%Y-%m-%d %H:%M:%S")
    currentdate = datetime.strptime(ETLTools.GetCurrentDatePreviousHour(), "%Y-%m-%d %H:%M:%S")
    
    print(currentdate)
    URL = f'https://api.exchange.coinbase.com/products/BTC-USD/candles?start={currentdate}&end={currentdate}&granularity=3600'
    r = requests.get(URL)
    data = r.json()
    print(data)

if __name__ == "__main__":
    E()
    T()
    L(cur, con)
    Clean()
    
CleanFiles = PythonOperator(
    task_id = 'CleanFiles',
    python_callable = Clean,
    dag = dag,
)

#>> CleanFiles

'''