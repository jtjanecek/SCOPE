import sys
import os
import glob

for f in glob.glob(os.path.join("logs", "*.log")):
	with open(f, 'r') as log:
		pid_to_kill = next(log).split("PID: ")[1]
	os.system("kill -2 {}".format(pid_to_kill))
