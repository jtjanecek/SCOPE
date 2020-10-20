import sys
sys.path.append("../")
from helpers.DateHelper import *
from helpers.DateHelper import TimestampToDate
import sqlite3
from sqlite3 import Error
from datetime import datetime
from datetime import timedelta
import time
import logging
import os
import pandas as pd


class HistoricalDB():
	def __init__(self, mode='rwc', db='Historical.db'):
		this_file_dir = os.path.dirname(os.path.abspath(__file__))

		this_file_dir = os.path.join(this_file_dir, "dbs")

		db_file = os.path.join(this_file_dir, db)
		db_file = "file:" + db_file + "?mode=" + mode
		logging.info("Using DB for orders: {}".format(db_file))

		# This will raise an error if it can't connect
		self.conn = sqlite3.connect(db_file, uri=True, check_same_thread=False)

		self._cols = ['dt_unix', 'symbol', 'open_price', 'close_price', 'vol']

		sql_create_min_table = """CREATE TABLE IF NOT EXISTS data (
									dt_unix real PRIMARY KEY,
									symbol text NOT NULL,
									open_price real NOT NULL,
									close_price real NOT NULL,
									high_price real NOT NULL,
									low_price real NOT NULL,
									vol real NOT NULL);
								"""

		c = self.conn.cursor()
		if mode != 'ro':
			c.execute(sql_create_min_table)

		sql = "CREATE UNIQUE INDEX IF NOT EXISTS sym_dt_idx ON data (dt_unix, symbol);"
		c.execute(sql)

		self.conn.commit()
		c.close()

	def _test(self, id):
		pass

	def insert(self, *args):
		if self.exists(args[0], args[1]):
			return

		c = self.conn.cursor()
		insert_command = """INSERT INTO data
							(dt_unix, symbol, open_price, close_price, high_price, low_price, vol)
							values(?,?,?,?,?,?,?);
							"""
		c.execute(insert_command, args)
		self.conn.commit()
		c.close()

	def exists(self, dt_unix, symbol):
		c = self.conn.cursor()
		select = """SELECT dt_unix
					FROM data
					WHERE dt_unix = ? AND symbol = ?;
				"""
		vals = c.execute(select, [dt_unix, symbol]).fetchone()
		c.close()
		if vals:
			return True
		return False

	def get(self):
		c = self.conn.cursor()
		select = """SELECT *
					FROM data;
				"""
		vals = c.execute(select).fetchall()
		c.close()
		# Check if it exists first
		if not vals:
			return None
		data = []
		for val in vals:
			data.append({'unix_dt': val[0], 'symbol': val[1], 'open_price': val[2],
						'close_price': val[3], 'high_price': val[4], 'low_price': val[5], 'vol': val[6]})
		df = pd.DataFrame(data)
		return df

