import threading


class Threader():
	
	def start(self):
		self._thread = threading.Thread(target=self._run)
		self._data_lock = threading.Lock()
		self._end_lock = threading.Lock()
		self._dead = False
		self._thread.start()

	def stopped(self):
		dead = False
		with self._end_lock:
			if self._dead:
				dead = True
		return dead

	def join(self):
		with self._end_lock:
			self._dead = True
		self._thread.join()
