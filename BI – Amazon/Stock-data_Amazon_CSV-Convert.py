
import json

rawdata = open("StockAPIdata.json", "r")

data = json.loads(rawdata.read())['data']

# text = rawdata.read()
# blob = json.loads(text)
# data = blob['data']

d = open("stockdata_amazon.csv", "w")

d.write("date,open,high,low,close,volume\n")

for record in data:
    date = record["date"]
    open = record["open"]
    high = record["high"]
    low = record["low"]
    close = record["close"]
    volume = record["volume"]
    d.write(f"{date},{open},{high},{low},{close},{volume}\n")

d.close()
