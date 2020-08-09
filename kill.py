import os
import glob

'''
This script kills the current running algorithm. Only for scripts running in the background.

Usage:
python kill.py
'''

for f in glob.glob(os.path.join("logs", "*.log")):
	with open(f, 'r') as log:
		pid_to_kill = next(log).split("PID: ")[1]
	os.system("kill -2 {}".format(pid_to_kill))
