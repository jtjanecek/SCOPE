import json
import urllib3
import sys
sys.path.append("..")
from keys import *
from datetime import datetime
import logging
logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s - %(module)s - %(levelname)s - %(message)s',
        datefmt='%m-%d-%y %H:%M')


from HistoricalDB import HistoricalDB

db = HistoricalDB()

def get_current_dt():
	res = ''
	with open('current_dt.txt', 'r') as f:
		res = f.read().strip()
	return res

def set_current_dt(new_dt):
	with open('current_dt.txt', 'w+') as f:
		f.write(str(new_dt))

def update_db(datapoint):	
	dt_unix = int(datapoint[0]/1000000000)
	most_recent_dt = dt_unix
	open_price = datapoint[1]
	close_price = datapoint[4]
	vol = datapoint[5]
	db.insert(dt_unix, 'XBT', open_price, close_price, vol)

def request_all():
	num_requests_left = 35
	
	logging.info("Requesting all ...")
	while (num_requests_left != 0):
		current_dt = get_current_dt()
		data = request_data(current_dt)
	
		num_requests_left = int(data['stats']['remainingTimebarQueries'])
		logging.info("Num requests left: {}".format(num_requests_left))

		most_recent_dt = ""
		for datapoint in data['values']:
			most_recent_dt = int(datapoint[0]/1000000000)
			update_db(datapoint)

		logging.debug("Final dt this batch: {}".format(datetime.fromtimestamp(int(most_recent_dt))))
		set_current_dt(most_recent_dt)
	logging.info("Finished all.")

def request_data(start, end='1602654132'):
	PADDING = "000000000"

	logging.debug("Requesting between:")
	logging.debug(datetime.fromtimestamp(int(start)))
	logging.debug(datetime.fromtimestamp(int(end)))
	
	start = str(int(start)) + PADDING
	end   = str(int(end)) + PADDING	

	URL = "https://cryptodatum.io/api/v1/candles/?symbol=bitfinex:btcusd&type={}&step={}&limit={}&start={}&end={}&format=json"
	URL = URL.format("time", "1m", 500, start, end)
	logging.debug("Requesting: {} ...".format(URL))

	http = urllib3.PoolManager()
	r = http.request('GET', URL, headers={"Authorization":CRYPTODATUM_API_KEY})
	data = r.data.decode('utf-8')
	return json.loads(data)

request_all()
'''
data = request_data(1364774868)

print(data['stats'])
print("Final DT:")
print(datetime.fromtimestamp(dt_unix))
'''
