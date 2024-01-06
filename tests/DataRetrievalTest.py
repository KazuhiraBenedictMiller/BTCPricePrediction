import requests
from datetime import datetime
product_id = "BTC-USD"
start = datetime.strptime("2023-06-01", "%Y-%m-%d")
end = datetime.strptime("2023-06-02", "%Y-%m-%d")

URL = f'https://api.exchange.coinbase.com/products/{product_id}/candles?start={start}&end={end}&granularity=3600'
r = requests.get(URL)
data = r.json()

print((end - start).total_seconds())
print(data)

