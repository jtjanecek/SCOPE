import sys, os
os.environ['PYTHONUNBUFFERED'] = 'True'
import signal
import time
import logging
from datetime import datetime, timedelta
from helpers.DateHelper import TimestampToDate
from helpers.Emailer import Emailer
import os.path
import os 
import json

from datastreams import *
from models import *


class Scope():
	def __init__(self, cli_args):
		# Setup config 
		self._config = cli_args
		with open(self._config['config'], 'r') as f:
			self._config.update(json.loads(f.read()))

		# Setup the logging
		self._setup_log(self._config['id'] + '.log', self._config['cli'])

		# Setup signal handler
		signal.signal(signal.SIGINT, self._signal_handler)
		logging.info("Using config: {config}".format(config=self._config))

		# Log PID
		logging.info("Using PID: {}".format(os.getpid()))

		# Setup emailer
		self._emailer = None
		if self._config['email']['enabled']:
			self._emailer = Emailer(config['sender_address'], config['sender_pass'], config['receiver_address']) 

		# Setup datastream
		logging.info("Starting datastream...")
		self.datastream = eval("{}.{}(self._config)".format(self._config['datastream'], self._config['datastream']))
		logging.info("Datastream started.")

		# Setup model
		logging.info("Initializing model...")
		self.model = eval("{}.{}(self._config)".format(self._config['model'], self._config['model']))
		logging.info("Model initialized.")

		# Start main loop
		try:
			self.main_routine()
		except SystemExit:
			raise
		except Exception as err:
			logging.exception(err)
			self._signal_handler(err,err)

	def main_routine(self):
		logging.info("ID: {}".format(self._config['id']))
		logging.info("Logfile: {}".format(self._config['id'] + '.log'))
		logging.info("Using config: {}".format(self._config))
		logging.info("Using datastream: {}".format(self.datastream))
		#logging.info("Using model: {}".format(model))

		# If there are no datapoints after 15 minutes, then just exit
		last_price_dt_recv = datetime.now()

		while True:
			dp, status = self.datastream.pop()
			### Update the model with the datapoint

			# Local datastream has been completed
			if self.datastream.version == 'Local' and status == 'No datapoint':
				logging.info("Datastream completed")
				self._signal_handler(2, None)

			# If there is a datapoint	
			if dp:
				# If it has been longer than 10 seconds from the timepoint, ignore it
				if self.datastream.version == 'Live' and datetime.now() - TimestampToDate(dp['dt']) > timedelta(seconds=10):
					logging.info("Ignoring {}.. received too late".format(dp))
					continue

				# Update the model
				logging.info("Got {}".format(dp))
				self.model.update(dp)
				last_price_dt_recv = datetime.now()
			elif datetime.now() - last_price_dt_recv > timedelta(minutes=75):
				logging.info("No datapoint received for 75 minutes. Exiting")
				self._signal_handler("No datapoint received in 75 minutes", None)
			time.sleep(.1)

	def _setup_log(self, log, cli):
		if cli == 'disable':
			logging.basicConfig(level=logging.DEBUG,
					format='%(asctime)s - %(module)s - %(levelname)s - %(message)s',
					datefmt='%m-%d-%y %H:%M',
					filename='logs/{}'.format(log),
					filemode='w')
		else: # Output to CLI
			logging.basicConfig(level=logging.DEBUG,
					format='%(asctime)s - %(module)s - %(levelname)s - %(message)s',
					datefmt='%m-%d-%y %H:%M')

	def _signal_handler(self, sig, frame):
		if sig != 2 and self._emailer != None:
			logging.info("Sending error email...")
			self._emailer.send("Error: {}".format(sig))
		logging.info("Sig: {}".format(sig))
		logging.info("Frame: {}".format(frame))
		logging.info("End signal detected...")
		logging.info("Joining datastream...")
		self.model.join()
		self.datastream.join()
		logging.info('Done!')
		sys.exit(0)



#########################################
############ PROCESS ARGS ###############
#########################################
import argparse

parser = argparse.ArgumentParser(description='Run the trading algo')
parser.add_argument('--config', help='config file for this algo', required=True)
parser.add_argument('--cli', help='enable CLI', default='enable')
cli_args = vars(parser.parse_args())


#########################################
############ Start Trader ###############
#########################################

Scope(cli_args)
