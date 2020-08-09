import os
import logging
from datetime import datetime
import sys
import websocket
import json
import time
import threading
sys.path.append("../")
from helpers.Threader import Threader
import urllib.request


'''
Get the datastream for PAXOS ItBit Cryptocurrency
'''

class DataLiveItBit(Threader):
	def __init__(self, config):
		# Set version to Live so that AlgoTrader knows how to handle this datastream
		self.version = 'Live'
		self._data = []
		self._previous_price = None

		# Start the Daemon
		self._thread = threading.Thread(target=self._run, daemon=True)
		self._data_lock = threading.Lock()
		self._thread.start()
	
	def stopped(self):
		pass

	def join(self):
		pass

	def pop(self):
		with self._data_lock:
			try:
				dataToReturn = self._data.pop(0)
			except IndexError:
				dataToReturn = None, 'No datapoint'
		return dataToReturn[0], dataToReturn[1]

	def _get_price(self):
		with urllib.request.urlopen('https://api.itbit.com/v1/markets/XBTUSD/ticker') as f:
			data = json.loads(f.read())
			price = float(data['lastPrice'])
			ask = float(data['ask'])
			bid = float(data['bid'])
			dt = datetime.now().timestamp()
			dp = {'dt': dt, 'price_recv': (ask+bid)/2}	
			if dp['price_recv'] != self._previous_price:
				self._previous_price = dp['price_recv']
				logging.debug("Recv: {}".format(dp))
				return [dp, 'OK']
			
	def _run(self):
		while True:
			try:
				datapoint = self._get_price()	
				if datapoint:
					with self._data_lock:
						self._data.append(datapoint)
			except Exception as inst:
				logging.error(inst)
			time.sleep(5)

if __name__ == "__main__":
	#websocket.enableTrace(True)
	datastream = DataItBit()
	datastream.start()
	while True:
		time.sleep(1)
