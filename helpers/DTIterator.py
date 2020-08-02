from helpers.DateHelper import *
from datetime import datetime, date, timedelta
import pandas as pd
from keys import BEGIN_MIN, BEGIN_HOUR, END_HOUR, END_MIN

# Iterate over every Date and Minute between them on
# working dates only
class DTIterator():
	def __init__(self, start_dt, end_dt):
		d_init = start_dt
		d_init = d_init.replace(hour=BEGIN_HOUR, minute=BEGIN_MIN)
		datetimes = []
		while d_init <= end_dt:
			datetimes.append(d_init)
			d_init += timedelta(minutes=1) 
			if d_init.hour >= END_HOUR and d_init.minute > END_MIN:
				d_init = d_init.replace(hour=BEGIN_HOUR, minute=BEGIN_MIN)
				d_init += timedelta(days=1)
		self.datetimes = [dt for dt in datetimes if dt.weekday() <= 4]

	def __iter__(self):
		for d in self.datetimes:
			yield d, DateToTimestamp(d)

	def getDaySet(self) -> set:
		'''
		Return a set of the "Days" to get
		'''
		s = set()
		for d in self.datetimes:
			s.add(date(d.year, d.month, d.day))
		return s

	def getWeekList(self) -> list:
		'''
		For each 'week' in the dates, return the start and end dates
		'''
		daySet = self.getDaySet()
		dayList = sorted(list(daySet))	
		weekList = []
		curWeekList = []
		for i, day in enumerate(dayList):
			if curWeekList == []:
				curWeekList.append(day)
			elif (day-curWeekList[-1]).days != 1:
				weekList.append(curWeekList)
				curWeekList = [day]
			else:
				curWeekList.append(day)
		if curWeekList != []:
			weekList.append(curWeekList)

		weekList = [(week[0], week[-1]) for week in weekList]
		return weekList

if __name__ == '__main__':
	startDate = datetime(2005,1,1)
	endDate   = datetime(2007,1,7)
	dateiter = DTIterator(startDate, endDate)
	for dt, timestamp in dateiter:
		print(dt, timestamp)

