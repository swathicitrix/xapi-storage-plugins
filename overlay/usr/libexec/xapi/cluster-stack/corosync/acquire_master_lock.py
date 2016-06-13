#!/usr/bin/python
# Try to acquire the lock that indicates which host is the master.
# If we successfully acquire the lock, but retain the lock forever. (If we leave the cluster, another host can get the lock.)
# 
# Usage:
#   acquire_master_lock.py <path-to-sr>

import fcntl
import sys
import time
import subprocess
import os
import errno
import json

def logger(msg):
	subprocess.call(["/usr/bin/logger", msg])

def mkdir(d):
	try:
		os.makedirs(d)
	except OSError as exc:
		if exc.errno == errno.EEXIST:
			pass
		else:
			raise

if len(sys.argv) < 2:
	print "Usage:  %s <path-to-sr>" % (sys.argv[0])
	sys.exit(1)

sr_path = sys.argv[1]
lock_dir = "%s/.ha/master" % (sr_path)
mkdir(lock_dir)
lock_path = "%s/lock" % (lock_dir)

p = subprocess.Popen(["/usr/bin/hostname"], stdout=subprocess.PIPE)
hostname = p.stdout.readline().rstrip()

logger("Trying to acquire lock %s for host '%s'..." % (lock_path, hostname))

# Try to acquire the lock
fd = open(lock_path, 'w+')
fcntl.flock(fd, fcntl.LOCK_EX)

logger("Acquired master lock!")

# We've got the lock; write to the state file (only the host owning the lock is permitted to do this)
state_file = "%s/state.json.tmp" % (lock_dir)

logger("Writing to %s ..." % (state_file))

o = {"master": hostname}
with open(state_file, 'w') as fd_s:
	json.dump(o, fd_s)

# Atomically replace the live state.json file
os.rename("%s/state.json.tmp" % (lock_dir), "%s/state.json" % (lock_dir))

logger("Holding master lock forever")

# Hold the lock forever
while True:
	time.sleep(3600)
