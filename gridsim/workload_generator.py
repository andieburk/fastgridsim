# a statistically robust dependent task set generator
# Andrew Burkimsher May/June 2011
# Department of Computer Science, University of York

import random
import numpy
import argparse
import sys


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



def n_uniform_ints_with_sum_simple(count, sum_nums):
	uni_floats = [random.random() for n in range(count)]
	floatsum = sum(uni_floats)
	multiplier = float(sum_nums) / floatsum
	uni_ints = [int(round(fl*multiplier)) for fl in uni_floats]
	
	sumints = sum(uni_ints)
		
	if sumints is not sum_nums: #because of rounding errors
		uni_ints[-1] = uni_ints[-1] + (sum_nums - sumints)
	
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
	
		valid = (last >= mintask) and (last <= maxtask)
		
	nums.append(last)
	#print 'iterations', iter
	return nums


def log_distrib_ints_with_sum(num, total_sum):
	
	numbersfloat = UUnifast(num, total_sum)
	
	numbersint = [int(round(i)) for i in numbersfloat]
	
	numberintsum = sum(numbersint)
	
	if numberintsum is not total_sum: #because of rounding errors
		numbersint[-1] = numbersint[-1] + (total_sum - numberintsum)
	
	return numbersint



#structural classes

class idcounter:
	def __init__(self):
		self.count = 0
	
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
	
	taskexeclengths = n_normal_ints_with_sum(num_tasks, area, 10, area/2)
	
	
	for i in range(len(res)):
		res[i].execution_time = taskexeclengths[i]
	
	return res


def generate_binary_diamond_block(edge_length, total_exec_sum):
	num_tasks = edge_length ** 2
	
	









def generate_independent_blocks(minchains, maxchains, minchainlength, maxchainlength, numjobs, totalexecsum, taskidgenerator):
	jobtimes = log_distrib_ints_with_sum(numjobs, totalexecsum)
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
	joblengths = log_distrib_ints_with_sum(num_jobs, totalexecsum)
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
		joblengths = log_distrib_ints_with_sum(num_jobs, totalexecsum)
	
	#print 'iterations', iterations
	
	for j in range(num_jobs):
		job = []
		numblocks = random.randrange(minblockchain,maxblockchain)
		blocklengths = n_uniform_ints_with_sum_simple(numblocks, joblengths[j])
		
		itertwo = 0
		while min(blocklengths) < min_reasonable_block_length:
			itertwo +=1
			if itertwo > 1000:
				print 'too many attempted iterations and no satisfiable chain\n add more total exec time and run again'
				sys.exit(1)
			blocklengths = n_uniform_ints_with_sum_simple(numblocks, joblengths[j])
		
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


def output_joblist_to_file(joblist, filename):
	outfile = open(filename, 'w')
	jid = 1
	for j in joblist:
		outfile.write("\n\nJob " + str(jid))
		for t in j:
			outfile.write('\n'+' '.join(["Task",str(t.taskid),str(t.execution_time),','.join([ti.taskid for ti in t.dependencies])]))
		jid += 1
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



print "workload generator"

print "initialising..."

taskidgen = idcounter()
inputvals = init_and_read_input_space()

print "setting random seed..."
#random.seed(inputvals.randomseed)

#print inputvals

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


#print joblist

#print '#####'
#for j in joblist:
#	for t in j:
#		print t.taskid,':', t.execution_time, ':', [ti.taskid for ti in t.dependencies]

print 'exporting to ' + inputvals.outputfile + '...'

output_joblist_to_file(joblist, inputvals.outputfile)

print 'generation finished.'
	
	
