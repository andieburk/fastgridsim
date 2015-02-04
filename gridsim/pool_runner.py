#pool runner

import random
import subprocess
import os
from multiprocessing import Pool


def detectCPUs():
	"""
	Detects the number of CPUs on a system. Cribbed from pp.
	"""
	# Linux, Unix and MacOS:
	if hasattr(os, "sysconf"):
		if os.sysconf_names.has_key("SC_NPROCESSORS_ONLN"):
			# Linux & Unix:
			ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
			if isinstance(ncpus, int) and ncpus > 0:
				return ncpus
		else: # OSX:
			return int(os.popen2("sysctl -n hw.ncpu")[1].read())
	# Windows:
	if os.environ.has_key("NUMBER_OF_PROCESSORS"):
			ncpus = int(os.environ["NUMBER_OF_PROCESSORS"]);
			if ncpus > 0:
				return ncpus
	return 1 # Default

def exec_one(exec_string):
	#iter = 0
	print "executing", exec_string
	res = os.system(exec_string)
	#while iter < 10 and (res != 0):
	#	res = os.system(exec_string)
	
def parallel_exec_strings(string_list):
	#for load balancing across cores
	random.shuffle(string_list)
	
	cpucount = detectCPUs()
	
	#if __name__ == '__main__':
	pool = Pool(processes=cpucount)
	pool.map(exec_one, string_list)
		
