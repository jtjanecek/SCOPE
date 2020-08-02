from datetime import datetime
import time, os
os.environ['TZ'] = 'America/New_York'
time.tzset()

def TimestampToDate(unix_timestamp, EST=True):
	return datetime.fromtimestamp(unix_timestamp)

def DateToTimestamp(dt):
	return int(datetime.timestamp(dt))

