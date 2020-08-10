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

'''
Datastream for local CSV data
'''

class DataLocal(Threader):
	def __init__(self, config):
		# Set versino to 'Live' so that AlgoTrader knows to restart stream if there are errors
		self.version = 'Local'
		self._config = config
		self._data = []
		
		# Read in the CSV and iterate over each datapoint
		with open(self._config['data'], 'r') as f:
			for line in f.readlines():
				line = line.strip()
				if 'price_recv' in line:
					continue
				price_recv = float(line.split(',')[0])
				dt = float(line.split(",")[1])
				self._data.insert(0, [{'price_recv': price_recv, 'dt': dt}, 'OK'])

	def start(self):
		pass

	def join(self):
		pass

	def pop(self):
		''' Method called from AlgoTrader. Get the thread lock and return the most
		recent datapoint
		'''
		try:
			dataToReturn = self._data.pop()	
		except IndexError:
			dataToReturn = None, 'No datapoint'
		return dataToReturn[0], dataToReturn[1]

