import os
os.environ['TZ'] = 'America/New_York'
from datetime import datetime
import sys
sys.path.append("../")
import websocket
import json
import time
from datetime import timedelta
import threading
import logging
from helpers.Threader import Threader
from helpers.DateHelper import TimestampToDate

'''
Datastream for getting real time crypto Datapoints from Binance
'''

class DataLiveItBit(Threader):
	def __init__(self, config):
		self._config = config
		self._orderbook = {'bids': {}, 'ask': {}}
		self.start()

	def start(self):
		''' Start the thread to read datapoints
		'''
		self._thread = threading.Thread(target=self._run, daemon=True)
		self._data_lock = threading.Lock()
		self._thread.start()
	
	def stopped(self):
		pass

	def join(self):
		pass

	def now(self):
		''' Method called from AlgoTrader. Get the thread lock and return the most
		recent datapoint
		'''
		with self._data_lock:
			if self._orderbook == None:
				return None
			max_bid = max(self._orderbook['bids'].keys())
			min_ask = min(self._orderbook['asks'].keys())
			dt = datetime.now().timestamp()
			datapoint = {'bid': max_bid, 'ask': min_ask, 'dt': dt}
		return datapoint

	def _run(self):
		# Setup client
		def on_message(ws, message):
		    logging.debug("Received: " + message)
		    message = json.loads(message)
		    if message['type'] == 'SNAPSHOT':
		    	self._orderbook['bids'] = {float(bid['price']): float(bid['amount']) for bid in message['bids']}
		    	self._orderbook['asks'] = {float(bid['price']): float(bid['amount']) for bid in message['asks']}
		    elif message['type'] == 'UPDATE':
		    	price = float(message['price'])
		    	amount = float(message['amount'])
		    	side = message['side']

		    	if side == 'BUY':
		    		if amount == 0:
		    			del self._orderbook['bids'][price]
		    		else:
		    			self._orderbook['bids'][price] = amount
		    	if side == 'SELL':
		    		if amount == 0:
		    			del self._orderbook['asks'][price]
		    		else:
		    			self._orderbook['asks'][price] = amount

		    max_bid = max(self._orderbook['bids'].keys())
		    min_ask = min(self._orderbook['asks'].keys())

		def on_error(ws, error):
			print(error)

		def on_close(ws):
		    print("### closed ###")

		def on_open(ws):
		    pass

		websocket.enableTrace(True)
		ws = websocket.WebSocketApp("wss://ws.paxos.com/marketdata/BTCUSD",
	                              on_message = on_message,
	                              on_error = on_error,
	                              on_close = on_close)
		ws.on_open = on_open
		ws.run_forever()

if __name__ == "__main__":
	datastream = DataLiveItBit([])
	for i in range(150):
		time.sleep(1)
		print(datastream.now())
	print("Ending!")
	datastream.join()
	print("Done!")
