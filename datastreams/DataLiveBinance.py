# Be sure to pip install websocket-clientAAQ
# Details: https://pypi.org/project/websocket-client/
import os
os.environ['TZ'] = 'America/New_York'
from datetime import datetime
import sys
import websocket
import json
import time
from datetime import timedelta
import threading
import logging
sys.path.append("../")
from helpers.Threader import Threader
from helpers.DateHelper import TimestampToDate

class DataLiveBinance(Threader):
	def __init__(self, config):
		self.version = 'Live'
		self._config = config
		self._data = []
		self.start()

	def start(self):
		self._thread = threading.Thread(target=self._run, daemon=True)
		self._data_lock = threading.Lock()
		self._thread.start()
	
	def stopped(self):
		pass

	def join(self):
		return

	def pop(self):
		with self._data_lock:
			try:
				dataToReturn = self._data.pop()	
			except IndexError:
				dataToReturn = None, 'No datapoint'
		return dataToReturn[0], dataToReturn[1]

	def _run(self):
		# Setup client
		from keys import BINANCE_API_KEY, BINANCE_SECRET_KEY
		from binance.client import Client
		client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
		from binance.websockets import BinanceSocketManager
		bm = BinanceSocketManager(client)

		# Run forever
		last_msg_dt = datetime.now().timestamp()
		def process_message(msg):
			timeReceived = datetime.now().timestamp()
			if msg['e'] == 'error':
				logging.error("Error: {}".format(msg))
			else:
				logging.debug("RECV: {}".format(msg))
				with self._data_lock:
					self._data.insert(0,[{'price_recv': float(msg['p']), 'dt': timeReceived}, 'OK'])
			
		# start any sockets here, i.e a trade socket
		conn_key = bm.start_aggtrade_socket('BTCUSDT',process_message)
		# then start the socket manager
		bm.start()

if __name__ == "__main__":
	#websocket.enableTrace(True)
	datastream = DataBinance()
	for i in range(150):
		time.sleep(1)
		if i % 10 == 0:
			print(i)
	print("Ending!")
	datastream.join()
	print("Done!")
'''
from keys import BINANCE_API_KEY, BINANCE_SECRET_KEY
from binance.client import Client
client = Client(BINANCE_API_KEY, BINANCE_SECRET_KEY)
from binance.websockets import BinanceSocketManager
bm = BinanceSocketManager(client)
def process_message(msg):
	if msg['e'] == 'error':
		print("Error: {}".format(msg))
	else:
		print("RECV:",msg)
# start any sockets here, i.e a trade socket
conn_key = bm.start_aggtrade_socket('BTCUSDT',process_message)
#conn_key = bm.start_aggtrade_socket('BNBBTC',process_message)
# then start the socket manager
bm.start()
'''

