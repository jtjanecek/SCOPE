import urllib3
import sys
sys.path.append("..")
from keys import *

from HistoricalDB import HistoricalDB

db = HistoricalDB()

def request_data(start, end):
	PADDING = "000000000"
	
	start = str(int(start)) + PADDING
	end   = str(int(end)) + PADDING	

	URL = "https://cryptodatum.io/api/v1/candles/?symbol=bitfinex:btcusd&type={}&step={}&limit={}&start={}&end={}&format=json"
	URL = URL.format("time", "1m", 500, start, end)
	print("Requesting:",URL, "...")

	http = urllib3.PoolManager()
	r = http.request('GET', URL, headers={"Authorization":CRYPTODATUM_API_KEY})
	data = r.data.decode('utf-8')
	return data

data = request_data("1364774868", "1365774868")

import datetime
for datapoint in data['values']:
	dt_unix = datapoint[0]/1000000000
	open_price = datapoint[1]
	close_price = datapoint[4]
	vol = datapoint[5]
	db.insert(dt_unix, 'XBT', open_price, close_price, vol)

