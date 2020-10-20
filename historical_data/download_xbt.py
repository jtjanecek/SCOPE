import json
import requests
import sys
sys.path.append("..")
from keys import *
from datetime import datetime
import logging
logging.basicConfig(level=logging.DEBUG,
        format='%(asctime)s - %(module)s - %(levelname)s - %(message)s',
        datefmt='%m-%d-%y %H:%M')


class CoinApiDownloader():
	def __init__(self, api_key):

		self._BASE_URL = 'https://rest.coinapi.io/v1/ohlcv/BITSTAMP_SPOT_BTC_USD/history?limit=100000&period_id={}&time_start={}'

		self._HEADERS = {'X-CoinAPI-Key' : api_key}

	def coinapi_dt_to_unix(self, coinapi_dt: str):
		dt = coinapi_dt[:-2].replace('T',' ')
		python_dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:00.000000')
		return python_dt.timestamp()

	def unix_to_coinapi_dt(self, unix_dt):
		return str(datetime.fromtimestamp(int(unix_dt))).replace(" ","T")

	def request_data(self, start):
		'''
		Params:
			start: unix epoch time
		Return:
			data: list of OHLC timepoints
		'''

		dt_start = self.unix_to_coinapi_dt(start)

		logging.debug("Requesting from: {}".format(dt_start))
	
		url = self._BASE_URL.format("1MIN", dt_start)
		logging.debug("Requesting: {} ...".format(url))

		response = requests.get(url, headers=self._HEADERS)
	
		# Too many requests status code
		if response.status_code == 429:
			return response.status_code, None

		data = response.json()
		logging.debug("Data length: {}".format(len(data)))

		return response.status_code, data


if __name__ == '__main__':
	downloader = CoinApiDownloader(COINAPI_API_KEY)

	from HistoricalDB import HistoricalDB
	db = HistoricalDB()
	cur_data = db.get()

	dt_unix = int(cur_data['unix_dt'].values[-1])

	status_code, data = downloader.request_data(dt_unix)

	for datapoint in data:
		timepoint = downloader.coinapi_dt_to_unix(datapoint['time_period_start'])
		db.insert(timepoint, 'XBT', datapoint['price_open'], datapoint['price_close'], datapoint['price_high'], datapoint['price_low'], datapoint['volume_traded'])
		print(datapoint)

