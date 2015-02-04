#runner.py

import os
import sys
from multiprocessing import Pool
import random
import matplotlib.pyplot
import matplotlib.pylab
import numpy
import scipy.stats
import math

#total sims = workload kinds *  workload count * number of utilisations * number of schedulers

workload_count = 30
for_real_stage = 0.0
analyse = True

processing_dir = "./all_run_files/"

verbose = False
if verbose:
	print "stage:", for_real_stage

u1 = range(60,80,10)
u2 = range(80,90,5)
u3 = range(90,110,2)
u4 = range(110,120,5)
u5 = range(120,200,20)
utilisations = []
utilisations.extend(u1)
utilisations.extend(u2)
utilisations.extend(u3)
utilisations.extend(u4)
utilisations.extend(u5)

#utilisations = [80,90,100,110,120]

schedulers = ["fifo_task", "fifo_job"]

numjobs = "20"
minchains = "1"
maxchains = "10"
minchainlength = "3"
maxchainlength = "10"
totalexecsum = "2000000"

maxblocks = "10"
minblocks = "1"

depprob = "0.6"




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


#generate single block workloads

def workload_gen(work_string):
	success = False
	count = 0
	while (not success) and (count < 10):
		a = os.system(work_string)
		if a == 0:
			return 0
		elif a == 2:
			print "generation failure"
			exit()
		elif a == 1:
			count += 1
	print "workload generation count exceeded, try increasing totalexecsum"
	exit()

#generate single blocks workloads
for i in range(1, workload_count+1):

	workload_command_list = [
		"python",
		"workload_generator.py",
		"--numjobs", numjobs,
		"--minchains", minchains,
		"--maxchains", maxchains,
		"--minchainlength", minchainlength,
		"--maxchainlength", maxchainlength,
		"--totalexecsum", totalexecsum,
		"--workloadstructure","1",
		"--outputfile", processing_dir + "workload_a_"+str(i)+".wld",
		]
	workload_command_string = ' '.join(workload_command_list)
	if verbose:
		print workload_command_string
	if for_real_stage >= 1.1:
		workload_gen(workload_command_string)
	
#generate linear blocks
for i in range(1, workload_count+1):

	workload_command_list = [
		"python",
		"workload_generator.py",
		"--numjobs", numjobs,
		"--minchains", minchains,
		"--maxchains", maxchains,
		"--minchainlength", minchainlength,
		"--maxchainlength", maxchainlength,
		"--totalexecsum", totalexecsum,
		"--workloadstructure","2",
		"--outputfile", processing_dir + "workload_b_"+str(i)+".wld",
		"--maxblocks", maxblocks,
		"--minblocks", minblocks,
		]
	workload_command_string = ' '.join(workload_command_list)
	if verbose:
		print workload_command_string
	if for_real_stage >= 1.2:
		workload_gen(workload_command_string)


#generate random block deps with fixed probability
for i in range(1, workload_count+1):

	workload_command_list = [
		"python",
		"workload_generator.py",
		"--numjobs", numjobs,
		"--minchains", minchains,
		"--maxchains", maxchains,
		"--minchainlength", minchainlength,
		"--maxchainlength", maxchainlength,
		"--totalexecsum", totalexecsum,
		"--workloadstructure","1",
		"--outputfile", processing_dir + "workload_c_"+str(i)+".wld",
		"--maxblocks", maxblocks,
		"--minblocks", minblocks,
		"--depprob", depprob
		]
	workload_command_string = ' '.join(workload_command_list)
	if verbose:
		print workload_command_string
	if for_real_stage >= 1.3:
		workload_gen(workload_command_string)


all_temp_files = os.listdir(processing_dir)

workload_files = [f for f in all_temp_files if (f[-3:] == "wld")]

simulations = []

for f in workload_files:
	for s in schedulers:
		for u in utilisations:
			simulation_params = ["python",
					     "sm.py",
					     s,
					     str(u),
					     processing_dir + f,
					     ">",
					     processing_dir+str(u)+"_"+s+"_"+f[:-4]+".run"
					     ]
			simulation_string = ' '.join(simulation_params)
			simulations.append(simulation_string)

print "simulation_count:", len(simulations)
if verbose:
	print simulations


cpucount = detectCPUs()
print "cpu count", cpucount

def run_simulation(simulation_exec_string):
	os.system(simulation_exec_string)

if for_real_stage >= 2.1:
	if __name__ == '__main__':
		pool = Pool(processes=cpucount)             
		pool.map(run_simulation, simulations[0:32]) 

if for_real_stage >= 2.2:
	
	#for load balancing across cores
	random.shuffle(simulations)
	
	if __name__ == '__main__':
		pool = Pool(processes=cpucount)             
		pool.map(run_simulation, simulations)
		


if analyse:
	run_pattern = processing_dir + "*.run"
	os.system("ls -l " + run_pattern + " > " + processing_dir + "filelist.txt")
	os.system("grep \#-\# " + run_pattern + " >  " + processing_dir + "filtered_run_output.txt")


	#read file to analyse
	res_file = open(processing_dir + "filtered_run_output.txt", 'r')
	results_lines = res_file.readlines()

	print len(results_lines)
	print results_lines[0:3]
	
	parsing_1 = [l.split(',') for l in results_lines]
	#print parsing_1
	parsing_2 = [l[0].split('/') for l in parsing_1]
	#print parsing_2
	parsing_3 = [j[2].split('_') for j in parsing_2]
	#print parsing_3
	
	utilisations_parsed = [p[0] for p in parsing_3]
	schedulers_parsed = ['_'.join(p[1:3]) for p in parsing_3]
	workload_parsed = ['_'.join(p[4] + p[5].split('.')[0]) for p in parsing_3]
	
	#print utilisations, schedulers, workload
	
	all_results = [[utilisations_parsed[i], schedulers_parsed[i], workload_parsed[i]]+parsing_1[i][1:] for i in range(len(parsing_1))]

	print all_results[0:3]
	print "len all results", len(all_results)
	
	
	results_interest_means = []
	results_interest_medians = []
	
	for s in schedulers:
		for u in utilisations:
			for t in ['a','b','c']:
				
				matches_means = [float(r[4]) for r in all_results if ((int(r[0]) == int(u)) and (s == r[1]) and (r[2][0] == t))]
				
				matches_medians = [float(r[6]) for r in all_results if ((int(r[0]) == int(u)) and (s == r[1]) and (r[2][0] == t))]
							
				results_interest_means.append([s, u, t, matches_means])
				results_interest_medians.append([s, u, t, matches_medians])
	
	print len(results_interest_means)
	print "expected len", len(schedulers) * len(utilisations) * 3
	
	print len(schedulers), len(utilisations)
	
	
	all_fifo_task_means = []
	all_fifo_job_means = []
	all_fifo_task_medians = []
	all_fifo_job_medians = []
	
	for i in range(len(results_interest_means)):
		
		if results_interest_means[i][0] == "fifo_task":
			all_fifo_task_means.extend(results_interest_means[i][3])
		if results_interest_medians[i][0] == "fifo_task":
			all_fifo_task_medians.extend(results_interest_means[i][3])
		
		if results_interest_means[i][0] == "fifo_job":
			all_fifo_job_means.extend(results_interest_means[i][3])
		if results_interest_medians[i][0] == "fifo_job":
			all_fifo_job_medians.extend(results_interest_means[i][3])
			

	#print all_fifo_task_means

	all_fifo_task_means = numpy.array(all_fifo_task_means)
	all_fifo_job_means =  numpy.array(all_fifo_job_means)
	all_fifo_task_medians =  numpy.array(all_fifo_task_medians)
	all_fifo_job_medians =  numpy.array(all_fifo_job_medians)
	
	
	
	#all_fifo_task_means = numpy.array([r[3] for r in results_interest_means if r[0] == "fifo_task"])
	#all_fifo_job_means = numpy.array([r[3] for r in results_interest_means if r[0] == "fifo_job" ])
	
	#print len(all_fifo_task_means), len(all_fifo_job_means)
	
	means_ratios = all_fifo_task_means / all_fifo_job_means
	medians_ratios = all_fifo_job_medians / all_fifo_task_medians
	
	print means_ratios[0:100]
	
	
	means_ratios_by_utilisation = []
	medians_ratios_by_utilisation = []
	
	
	for u in utilisations:
		means_ratios_by_utilisation.append(\
		 [means_ratios[i] for i in range(len(means_ratios)) 
		  if (results_interest_means[int(math.floor(float(i)/workload_count))][1] == u)])
		  
		medians_ratios_by_utilisation.append(\
		 [medians_ratios[i] for i in range(len(medians_ratios)) 
		  if (results_interest_medians[int(math.floor(float(i)/workload_count))][1] == u)])
	
	
	
	matplotlib.pyplot.boxplot(medians_ratios_by_utilisation)
	matplotlib.pyplot.show()
	
	print "analysis finished"
	
	"""
	exit()
	
	
	all_fifo_task_by_workload_kind = {}
	all_fifo_job_by_workload_kind = {}
	
	for workload_kind in ['a','b','c']:
		all_fifo_task_by_workload_kind[workload_kind] = [r[3] for r in results_interest_means 
								 if (r[0] == "fifo_task") and (r[2] == workload_kind)]
		all_fifo_job_by_workload_kind[workload_kind] = [r[3] for r in results_interest_means 
								 if (r[0] == "fifo_job") and (r[2] == workload_kind)]
								 
	
	
	
	
	
	print len(a), len(b)
	#	print a, b
	
	ameans = [numpy.mean(x) for x in a]
	amedians = [numpy.median(x) for x in a]
	amins = [numpy.min(x) for x in a]
	amaxs = [numpy.max(x) for x in a]
	
	bmeans = [numpy.mean(x) for x in b]
	bmedians = [numpy.median(x) for x in b]
	bmins = [numpy.min(x) for x in b]
	abaxs = [numpy.max(x) for x in b]
	
	
	fig = matplotlib.pylab.figure()

	matplotlib.pylab.errorbar(utilisations, amedians)
	matplotlib.pylab.errorbar(utilisations, bmedians)
	
	
	

	matplotlib.pylab.show()
	
	sigs = [scipy.stats.mannwhitneyu(a[i],b[i])[1]<0.05 for i in range(len(a))]
	ranksums = [scipy.stats.ranksums(a[i],b[i]) for i in range(len(a))]

	print sigs, ranksums
	
	
	#matplotlib.pyplot.boxplot(a)
	#matplotlib.pyplot.boxplot(b)
	matplotlib.pyplot.show()

	"""










