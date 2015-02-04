# a statistically robust dependent task set generator
# Andrew Burkimsher May/June 2011
# Department of Computer Science, University of York

import random
import numpy
import argparse
import sys
from value_curve_generation import random_curve


# distributions

def UUnifast(number_of_results, U):
	vectU = []
	sumU = U
	for i in range(0, number_of_results-1):
		nextSumU = sumU * random.random() ** (1.0 / float(number_of_results - i))
		vectU.append(sumU - nextSumU)
		sumU = nextSumU
		#print sumU
	vectU.append(sumU)
	return vectU



def n_uniform_ints_with_sum_simple(count, sum_nums, mintask, maxtask):
	
	if count == 1:
		return [sum_nums]
	
	
	maxiter = 1000
	iter = 0
	valid = False
	
	while not valid:	
		
		uni_floats = [random.random() for n in range(count)]
		floatsum = sum(uni_floats)
		multiplier = float(sum_nums) / floatsum
		uni_ints = [int(round(fl*multiplier)) for fl in uni_floats]
		
		sumints = sum(uni_ints)
			
		if sumints is not sum_nums: #because of rounding errors
			uni_ints[-1] = uni_ints[-1] + (sum_nums - sumints)
	
	
		valid = (min(uni_ints) >= mintask) and (max(uni_ints) <= maxtask)
		iter +=1
		if iter > maxiter:
			print "iteration count exceeded in n_uniform_ints.....simple"
			exit()
	
	return uni_ints
		

def n_normal_ints_with_sum(count, sum_nums, mintask, maxtask):
	mean = float(sum_nums)/float(count)
	#print 'mean', mean
	
	sda = (mean - mintask) / 3.0
	sdb = (maxtask - mean) / 3.0

	if (sda <= 0) or (sdb <= 0):
		print "range does not include mean,\n it is likely that too low a workload total sum\n has been given for the number of tasks\n or just retry generation"
		print 'count', count
		print 'sum_nums', sum_nums
		print 'mintask', mintask
		print 'maxtask', maxtask
		print 'mean', mean
		print 'sda', sda
		print 'sdb', sdb
		exit(1)

	
	std_dev = min([sda, sdb])
	
	#print std_dev, mean
	
	valid = False
	iter = 0
	while not valid:
		iter += 1
		nums_float = numpy.random.normal(mean, std_dev, count-1)
		
		nums = [abs(int(round(n))) for n in nums_float]
	
		last = sum_nums - sum(nums)
	
		valid = (last >= mintask) and (last <= maxtask)
		
	nums.append(last)
	#print 'iterations', iter
	return nums




def n_uniform_ints_with_sum(count, sum_nums, mintask, maxtask):
	
	#do some validation on requested inputs
	if (sum_nums < (count * mintask)) or (sum_nums > count*maxtask):
		print "invalid inputs in n_uniform_ints_with_sum"
		sys.exit(2)
	
	mean_one = float(mintask + maxtask) / 2.0
	mean_two = float(sum_nums) / float(count)
	means_diff_ratio = abs(mean_one - mean_two) / (mean_one + mean_two)
	
	#print "means diff", means_diff_ratio
	
	if means_diff_ratio > 0.1:
		print "warning, parameters to n uniform ints with sum routine may be flawed and out of suitable range"
		sys.exit(2)
	
	valid = False
	iter = 0100
	while not valid:
		iter += 1
		nums = [random.randrange(mintask, maxtask) for i in range(count-1)]
			
		last = sum_nums - sum(nums)
		min_nums = min(nums)
		e = nums.copy().append(last)
		
		
		valid = ((last >= mintask) and (last <= maxtask)) and (min_nums >= 1)
		
	nums.append(last)
	#print 'iterations', iter
	return nums


def log_distrib_ints_with_sum(num, total_sum, mintask, maxtask):
	
	numbersfloat = UUnifast(num, total_sum)
	
	numbersint = [int(round(i)) for i in numbersfloat]
	
	numberintsum = sum(numbersint)
	
	if numberintsum is not total_sum: #because of rounding errors
		numbersint[-1] = numbersint[-1] + (total_sum - numberintsum)
	
	return numbersint



#structural classes

class idcounter:
	def __init__(self):
		self.count = 1
	
	def newid(self):
		c = self.count
		self.count += 1
		return c


class task_basic():
	def __init__(self, taskid):
		self.taskid = taskid
		self.dependencies = []
		self.execution_time = -1
	def self_print(self):
		print self.taskid, [t.taskid for t in self.dependencies]
		

class branching_unit_structure():
	def __init__(self, branching_factor, chain_length, area):
		self.branching_factor = branching_factor
		self.chain_length = chain_length
		self.innertasklist = generate_branching_inner_block_by_area(branching_factor, chain_length, area)
	
		

#generators

def generate_branching_inner_block_by_area(branching_factor, chain_length, area, id_generator):
	num_tasks = chain_length * branching_factor
	
	tasklist = [[task_basic(str(id_generator.newid())) for j in range(branching_factor)] for i in range(chain_length)]
	
	for i in range(1, chain_length):
		for j in range(branching_factor):
			tasklist[i][j].dependencies.append(tasklist[i-1][j])
	
	
	tlflat = [tasklist[i][j] for i in range(chain_length) for j in range(branching_factor)]
	
	return tlflat
			

def generate_branching_block(branching_factor, chain_length, area, id_generator):

	init_task = task_basic(str(id_generator.newid()))
	
	tasklist = generate_branching_inner_block_by_area(branching_factor, chain_length, area, id_generator)
	
	final_task = task_basic(str(id_generator.newid())) 

	for t in tasklist[0:branching_factor]:
		t.dependencies.append(init_task)
	
	final_task.dependencies = [tasklist[-i] for i in range(1, branching_factor+1)]
	
	res = [init_task]
	res.extend(tasklist)
	res.append(final_task)
	
	num_tasks = len(res)
	
	taskexeclengths = n_normal_ints_with_sum(num_tasks, area, 1, area/2)
	
	
	for i in range(len(res)):
		res[i].execution_time = taskexeclengths[i]
	
	return res


def generate_binary_diamond_block(edge_length, total_exec_sum):
	num_tasks = edge_length ** 2
	
	









def generate_independent_blocks(minchains, maxchains, minchainlength, maxchainlength, numjobs, totalexecsum, taskidgenerator):
	jobtimes = log_distrib_ints_with_sum(numjobs, totalexecsum, None, None)
	joblist = [generate_branching_block(\
					random.randrange(minchains, maxchains),\
					random.randrange(minchainlength, maxchainlength),\
					jobtime,\
					taskidgenerator) for jobtime in jobtimes]
	
	return joblist


def generate_linear_chained_blocks(minchains, maxchains, minchainlength, maxchainlength, minblockchain, maxblockchain, num_jobs, totalexecsum, taskidgenerator):

	chain_length_per_job = [random.randrange(minblockchain, maxblockchain) for i in range(num_jobs)]
	total_blocks = sum(chain_length_per_job)
	
	job_total_exec_times = log_distrib_ints_with_sum(total_blocks, totalexecsum)
	
	blocklist = [generate_branching_block(\
					random.randrange(minchains, maxchains),\
					random.randrange(minchainlength, maxchainlength),\
					jobtime,\
					taskidgen) for jobtime in job_total_exec_times]
					
	
	joblist = []

	for j in range(num_jobs):
		joblist.append([])
		#print joblist
		jcount = 1
		for i in range(chain_length_per_job[j]):
			curr_block = blocklist.pop(0)
			if jcount > 1:
				curr_block[0].dependencies.append(joblist[j][-1])
			joblist[j].extend(curr_block)
			jcount += 1	

	return joblist


def generate_blocks_unstitched(minchains, maxchains, minchainlength, maxchainlength, minblockchain, maxblockchain, num_jobs, totalexecsum, taskidgenerator):
	joblengths = log_distrib_ints_with_sum(num_jobs, totalexecsum, None, None)
	joblist = []
	
	min_reasonable_job_length = maxchains * maxchainlength * maxblockchain * 10
	min_reasonable_block_length = int(float(min_reasonable_job_length)/(float(minblockchain*5)))
	print 'minimum job length threshold', min_reasonable_job_length
	print 'minimum block length threshold', min_reasonable_block_length
	
	iterations = 0
	while min(joblengths) < min_reasonable_job_length:
		iterations +=1
		if iterations > 1000:
			print 'too many attempted iterations and no satisfiable chain\n add more total exec time and run again'
			sys.exit(1)
		joblengths = log_distrib_ints_with_sum(num_jobs, totalexecsum, None, None)
	
	#print 'iterations', iterations
	
	for j in range(num_jobs):
		job = []
		numblocks = random.randrange(minblockchain,maxblockchain)
		blocklengths = n_uniform_ints_with_sum_simple(numblocks, joblengths[j], 1, 100000000)
		
		itertwo = 0
		while min(blocklengths) < min_reasonable_block_length:
			itertwo +=1
			if itertwo > 1000:
				print 'too many attempted iterations and no satisfiable chain\n add more total exec time and run again'
				sys.exit(1)
			blocklengths = n_uniform_ints_with_sum_simple(numblocks, joblengths[j], 1, 100000000)
		
		#print 'blocklengths', blocklengths
		
		blocks = [generate_branching_block(random.randrange(minchains, maxchains),\
						   random.randrange(minchainlength, maxchainlength),\
						   blocktime,\
						   taskidgenerator)\
			  for blocktime in blocklengths]
		
		#for b in range(1, len(blocks)):
		#	blocks[b][0].dependencies.append(blocks[b-1][-1])
		
		for b in blocks:
			job.append(b)
		
		joblist.append(job)
		
	return joblist

def generate_linear_chained_blocks_vthree(minchains, maxchains, minchainlength, maxchainlength, minblockchain, maxblockchain, num_jobs, totalexecsum, taskidgenerator):
	
	jobsandblocks = generate_blocks_unstitched(minchains, maxchains, minchainlength, maxchainlength, minblockchain, maxblockchain, num_jobs, totalexecsum, taskidgenerator)
	
	joblist = []
	
	for jblocks in jobsandblocks:
		
		for b in range(1,len(jblocks)):
			jblocks[b][0].dependencies.append(jblocks[b-1][-1])
	
		job = []
		
		for block in jblocks:
			job.extend(block)
		
		joblist.append(job)
	
	return joblist
			

def generate_random_dependent_blocks(minchains, maxchains, minchainlength, maxchainlength, minblockchain, maxblockchain, depprob, num_jobs, totalexecsum, taskidgenerator):
	
	jobsandblocks = generate_blocks_unstitched(minchains, maxchains, minchainlength, maxchainlength, minblockchain, maxblockchain, num_jobs, totalexecsum, taskidgenerator)
	
	joblist = []
	
	for jblocks in jobsandblocks:
		for b in range(1, len(jblocks)):
			newdeps = [jblocks[i][-1] for i in range(0,b) if random.random() <= depprob]
			
			jblocks[b][0].dependencies.extend(newdeps)
	
		job = []
		
		for block in jblocks:
			job.extend(block)
		
		joblist.append(job)
	
	return joblist


def job_with_dep_tasks(task_time_distribution, dep_prob, mintasks_per_job, maxtasks_per_job, min_task_exec_time, max_task_exec_time, job_exec_sum, taskidgenerator):

	#print dep_prob, mintasks_per_job, maxtasks_per_job, min_task_exec_time, max_task_exec_time, job_exec_sum

	numtasks = random.randrange(mintasks_per_job, maxtasks_per_job+1)
	
	#print numtasks
	
	tasklist = [task_basic(taskidgenerator.newid()) for i in range(numtasks)]
	task_execs = task_time_distribution(numtasks, job_exec_sum, min_task_exec_time, max_task_exec_time)
	
	for index in range(len(tasklist)):
		tasklist[index].dependencies = [t for t in tasklist[0:index] if random.random() <= dep_prob]
		tasklist[index].execution_time = task_execs[index]
	
	return tasklist
	
		
	
	



def generate_jobs_with_random_dependent_tasks(job_time_distribution, task_time_distribution, dep_prob, mintasks_per_job, maxtasks_per_job, numjobs, totalexecsum, taskidgenerator):
	#distributions can be 'uniform', 'normal' or 'logarithmic'
	#logarithmic means lots of all small tasks, a few big ones
	
	task_distrib = task_time_distribution
	
	jobtimes = job_time_distribution(numjobs, totalexecsum, 1, totalexecsum) #job_time_distribution(...
	
	#print jobtimes
	
	#print n_uniform_ints_with_sum_simple(12, jobtimes[0], None, None)
	
	tasktimes = log_distrib_ints_with_sum
	
	
	#job_with_dep_tasks(task_time_distribution, dep_prob, mintasks_per_job, maxtasks_per_job, min_task_exec_time, max_task_exec_time, job_exec_sum, taskidgenerator):
	
	
	joblist = [job_with_dep_tasks(\
					task_distrib,\
					dep_prob,\
					mintasks_per_job,\
					maxtasks_per_job, \
					1,\
					totalexecsum,\
					jobtime,\
					taskidgenerator) for jobtime in jobtimes]
	
	return joblist
	
	
	
	
	
	
	#-------------------- working here ----------------------------
	

def generate_independent_tasks_one_per_job(numjobs, distributions, totalexecsum):
	pass
	#also working here
	

def job_sum_of_texecs(j):
	return sum([t.execution_time for t in j])


def set_job_start_times_to_utilisation(joblist, utilisation, platform_core_count):

	num_procs = platform_core_count
	
	job_start_times = [0]

	
	new_start_time = 0
	prev_job_exec_sum = job_sum_of_texecs(joblist[0])
	for j in joblist:
		job_start_times.append(new_start_time)
		new_start_time += int(
							  (float(prev_job_exec_sum)/float(num_procs))
							  /
							  (float(utilisation)/float(100))
							 )
		prev_job_exec_sum = job_sum_of_texecs(j)

	return job_start_times









#def generate_linear_chained_blocks_better(minchains, maxchains, minchainlength, maxchainlength, minblockchain, maxblockchain, num_jobs, totalexecsum, taskidgenerator):
#	
#	joblengths = log_distrib_ints_with_sum(num_jobs, totalexecsum)
#	joblist = []
#	
#	min_reasonable_job_length = maxchains * maxchainlength * maxblockchain * 10
#	min_reasonable_block_length = int(float(min_reasonable_job_length)/(float(minblockchain*5)))
#	print 'minimum job length threshold', min_reasonable_job_length
#	print 'minimum block length threshold', min_reasonable_block_length
#	
#	iterations = 0
#	while min(joblengths) < min_reasonable_job_length:
#		iterations +=1
#		if iterations > 100:
#			print 'too many attempted iterations and no satisfiable chain\n add more total exec time and run again'
#			exit()
#		joblengths = log_distrib_ints_with_sum(num_jobs, totalexecsum)
#	
#	#print 'iterations', iterations
#	
#	for j in range(num_jobs):
#		job = []
##		numblocks = random.randrange(minblockchain,maxblockchain)
#		blocklengths = n_uniform_ints_with_sum_simple(numblocks, joblengths[j])
#		
#		itertwo = 0
#		while min(blocklengths) < min_reasonable_block_length:
#			itertwo +=1
#			if itertwo > 100:
#				print 'too many attempted iterations and no satisfiable chain\n add more total exec time and run again'
#				exit()
#			blocklengths = n_uniform_ints_with_sum_simple(numblocks, joblengths[j])
#		
#		#print 'blocklengths', blocklengths
#		
#		blocks = [generate_branching_block(random.randrange(minchains, maxchains),\
#						   random.randrange(minchainlength, maxchainlength),\
#						   blocktime,\
#						   taskidgenerator)\
#			  for blocktime in blocklengths]
#		
#		for b in range(1, len(blocks)):
#			blocks[b][0].dependencies.append(blocks[b-1][-1])
#		
#		for b in blocks:
#			job.extend(b)
#		
#		joblist.append(job)
#		
#	return joblist
#	

#def generate_linear_chained_blocks_better(minchains, maxchains, minchainlength, maxchainlength, minblockchain, maxblockchain, num_jobs, totalexecsum, taskidgenerator):



#output


def print_deps_list(deps_list):
	ts = [j for i in deps_list for j in i]
	for t in ts:
		t.self_print()

#def task_execs_for_job(j):
#	return [t.execution_time for t in 


def possible_curves_from_file(curves_filename):
	
	curvenames = []
	
	filehandle = open(curves_filename, 'r')
	
	for line in filehandle:
		lsplit = line.split(' ')
		if len(lsplit) > 1:
			if lsplit[0] == "Curve":
				curvenames.append(lsplit[1].rstrip())
	
	return curvenames




def output_joblist_to_file(joblist, cores, filename):


	kinds = ["Kind1"]

	#utils = [80,90,100,110,120]
	#platform_core_count = 10000
	

	outfile = open(filename + ".txt" , 'w')
	
	shares = ["/root/user1","/root/user2","/root/user3","/root/user4","/root/user5"]
	
	poss_curve_names = possible_curves_from_file("big_curves_file.txt")
	
	jid = 1
	for j in joblist:
		outfile.write("\n\nJob j" + str(jid) + " -2 " + random.choice(shares) + " " + random.choice(poss_curve_names))
		for t in j:
			if len(t.dependencies) > 0:
				#print t.dependencies
				depnames = ','.join(["t"+str(ti.taskid) for ti in t.dependencies])
				#print depnames
			else:
				depnames = " "
			#print depnames, type(depnames)
			
			outstr = ' '.join(["Task",
							   "t"+str(t.taskid),
							   random.choice(cores),
							   str(t.execution_time),
							   random.choice(kinds),
							   depnames])
			#print outstr
			
			outfile.write('\n'+outstr)
		jid += 1
	outfile.close()


def start_time(prev_job_exec_sum, num_procs, utilisation):
	

	new_start_time += int(
						  (float(prev_job_exec_sum)/float(num_procs))
						  /
						  (float(utilisation)/float(100))
						  )
	prev_job_exec_sum = j.sum_of_task_exectimes()




def output_joblist_to_hash_format(joblist, cores, filename):
	
	prev_job_exec_sum = 0
	new_start_time = 0
	num_procs = 2800
	utilisation = 120
	
	
	outfile = open(filename + ".txt" , 'w')

	jobcounter = 1
	taskcounter = 1
	
	
	for j in joblist:
		num_tasks = str(len(j))
		job_exec_sum = sum([t.execution_time for t in j])

		new_start_time += int(
							  (float(prev_job_exec_sum)/float(num_procs))
							  /
							  (float(utilisation)/float(100))
							  )
		prev_job_exec_sum = job_exec_sum
		
		
		for t in j:
			out_bits = []
			out_bits.append(str(jobcounter))
			out_bits.append(str(t.taskid))
			out_bits.append(num_tasks)
			out_bits.append(str(random.choice(cores)))
			out_bits.append(str(t.execution_time/100000.0))
			out_bits.append(str(job_exec_sum/100000.0))
			deps_list_short = t.dependencies[:5]
			
			deps_bits_list = []
			for i in range(5):
				if i < len(deps_list_short):
					deps_bits_list.append(deps_list_short[i].taskid)
				else:
					deps_bits_list.append(-1)
			
			deps_string = '/'.join([str(id) for id in deps_bits_list])

			out_bits.append(str(new_start_time/100000.0))
			out_bits.append(deps_string)
			
			
			out_string = '\t'.join(out_bits)
			outfile.write(out_string + '\n')
			taskcounter += 1
		
		
		

		jobcounter += 1
		
	outfile.close()





#input

def init_and_read_input_space():
	inputparser = argparse.ArgumentParser(description = "A taskset generator")
	inputparser.add_argument('--numjobs', type=int, help='the number of jobs to be generated')
	inputparser.add_argument('--totalexecsum', type=int, help='the workload execution time sum')
	inputparser.add_argument('--minchains', type=int, help='the minimum number of chains in a block')
	inputparser.add_argument('--maxchains', type=int, help='the maximum number of chains in a block')
	inputparser.add_argument('--minchainlength', type=int, help='the minimum length of a chain in a block')
	inputparser.add_argument('--maxchainlength', type=int, help='the maximum length of a chain in a block')
	inputparser.add_argument('--outputfile', type=str, help='the output file name')
	inputparser.add_argument('--minblocks', type=int, help='the minimum number of blocks per job (not used for independent blocks)')
	inputparser.add_argument('--maxblocks', type=int, help='the minimum number of blocks per job (not used for independent blocks)')
	inputparser.add_argument('--depprob', type=float, help='the 0.0-1.0 probability of dependency (only used for probabilistic dependency block pattern)')
	inputparser.add_argument('--workloadstructure', type=int, help='the structure of the workload. 1 corresponds to independent blocks, 2 to linear chains of blocks, 3 to probabilistically dependent blocks')
	inputparser.add_argument('--randomseed', type=int, help='a random seed. this is so replication is possible')
	inputs = inputparser.parse_args()
	return inputs




# ------------------------ main progam ----------------------

if __name__ == '__main__':
	
	print "workload generator"

	print "initialising..."

	#taskidgen = idcounter()
	#inputvals = init_and_read_input_space()

	print "setting random seed..."
	#random.seed(inputvals.randomseed)

	#print inputvals


	#generate_jobs_with_random_dependent_tasks(job_time_distribution, task_time_distribution, dep_prob, mintasks_per_job, maxtasks_per_job, numjobs, totalexecsum, taskidgenerator):

	#coreslist = [ [random.choice(cores) for i in j] for j in joblist]
		


	"""


	print "generating..."

	if inputvals.workloadstructure is 1:	
		joblist = generate_independent_blocks(inputvals.minchains,\
							 inputvals.maxchains,\
							 inputvals.minchainlength,\
							 inputvals.maxchainlength,\
							 inputvals.numjobs,\
							 inputvals.totalexecsum,\
							 taskidgen)

	elif inputvals.workloadstructure is 2:
		joblist = generate_linear_chained_blocks_vthree(inputvals.minchains,\
								inputvals.maxchains,\
								inputvals.minchainlength,\
								inputvals.maxchainlength,\
								inputvals.minblocks,\
								inputvals.maxblocks,\
								inputvals.numjobs,\
								inputvals.totalexecsum,\
								taskidgen)

	elif inputvals.workloadstructure is 3:
		joblist = generate_random_dependent_blocks(inputvals.minchains,\
								inputvals.maxchains,\
								inputvals.minchainlength,\
								inputvals.maxchainlength,\
								inputvals.minblocks,\
								inputvals.maxblocks,\
								inputvals.depprob,\
								inputvals.numjobs,\
								inputvals.totalexecsum,\
								taskidgen)

	else:
		print "invalid workload structure specified, it can be 1, 2 or 3."
		sys.exit(2)



	"""
	#print joblist

	#print '#####'
	#for j in joblist:
	#	for t in j:
	#		print t.taskid,':', t.execution_time, ':', [ti.taskid for ti in t.dependencies]

	#10000000000
	#10000000000

	#print 'exporting to ' + inputvals.outputfile + '...'

	for i in range(30):
		print "generating ", i
		taskidgen = idcounter()
		
		#joblist = generate_jobs_with_random_dependent_tasks(log_distrib_ints_with_sum, n_uniform_ints_with_sum_simple, 0.3, 1, 20, 4000, 100000000000, taskidgen)
		
		#joblist = generate_independent_blocks(1, 10, 1, 20, 1000, 10000000000, taskidgen)
		
		#generate_linear_chained_blocks_vthree(minchains, maxchains, minchainlength, maxchainlength, minblockchain, maxblockchain, num_jobs, totalexecsum, taskidgenerator)
		
		joblist = generate_linear_chained_blocks_vthree(1, 5, 1, 10, 1, 5, 1000, 10000000000, taskidgen)
		
		
		
		
		cores = ["1","5","10","15","20"]
		#output_joblist_to_file(joblist, cores, "/Users/andy/Documents/Work/workloads_march_13/synthetics/"+ "log_distrib_random_deps_" + str(i) + ".txt")
		
							
							
		output_joblist_to_hash_format(joblist, cores, "/Users/andy/Desktop/hash workloads/120 percent/chained blocks/workload_" + str(i))
	print 'generation finished.'
		
		
