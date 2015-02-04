import random
import numpy
#import argparse
#import sys
from value_curve_generation import random_curve
#import matplotlib.pyplot as plt
import math
from multiprocessing import Pool
import cProfile
from inter_arrivals import jsc2
import collections

# ------------------- distribution generators --------------------


def UUnifast_int(number_of_results, U):
	vectU = []
	sumU = U
	for i in range(0, number_of_results-1):
		nextSumU = int(sumU * random.random() ** (1.0 / float(number_of_results - i)))
		vectU.append(sumU - nextSumU)
		sumU = nextSumU
	#print sumU
	vectU.append(sumU)
	
	#print (sum(vectU)) == U
	return vectU


class distribution_generator():
	def __init__(kind, kind_parameters):
		self.kind_parameters = kind_parameters
		if kind == 'uunifast':
			self.out_distrib = self.UUnifast_int
		elif kind == 'log_linear':
			self.out_distrib = self.log_linear
		else:
			print "unknown kind in distribution generator"
			exit()



	def generate_distribution(number_of_results, result_execution_time_sum):
		
		pass


	def UUnifast_int(number_of_results, U):
		vectU = []
		sumU = U
		for i in range(0, number_of_results-1):
			nextSumU = int(sumU * random.random() ** (1.0 / float(number_of_results - i)))
			vectU.append(sumU - nextSumU)
			sumU = nextSumU
		#print sumU
		vectU.append(sumU)
		
		print (sum(vectU)) == U
		return vectU

	
	
	def UUnifast_int_log(number_of_results, U):
		vectU = []
		sumU = U
		for i in range(0, number_of_results-1):
			nextSumU = int(sumU * random.random() ** (1.0 / float(number_of_results - i)))
			
			
			
			vectU.append(sumU - nextSumU)
			sumU = nextSumU
		#print sumU
		vectU.append(sumU)
		
		print (sum(vectU)) == U
		return vectU

	def log_linear_final(number_of_results, sum_of_results):
		pass



def ll4():
	num_res = 1000
	set_total = 100000
	airb_total = 392601434340
	
	m =  3.83 * (10 ** -5)
	c = 0.569

	samples = numpy.random.exponential(scale = 0.001, size=1)

	samples.sort()
	plt.plot(numpy.log10(samples))
	plt.show()







def ll3():
	num_res = 1000
	set_total = 100000
	airb_total = 392601434340
	
	m =  3.83 * (10 ** -5)
	c = 0.569
	
	
	bases = numpy.random.uniform(0, 134461, num_res)
	
	#bases = numpy.array(range(134461))
	
	
	ys = (m* bases) + c


	bigs = numpy.power(10, ys)

	bigs.sort()


	cs = numpy.cumsum(bigs)
	
	print cs[-30:]

	find_index = len(bigs) - 1

	while cs[find_index] > set_total:
		find_index += -1

	print find_index

	all_inc_max = bigs[:find_index]

	aac_rounded = numpy.ceil(all_inc_max)

	print numpy.sum(all_inc_max)
	print cs[find_index]



	



def log_linear_2():
	num_res = 1000
	set_total = 100000
	airb_total = 392601434340

	m =  3.83 * (10 ** -5)
	c = 0.569


	bases = numpy.random.uniform(0, 134461, num_res)
	
	#bases = numpy.array(range(134461))
	
	
	ys = (m* bases) + c
	
	
	bigs = numpy.power(10, ys)
	
	actual_total = numpy.sum(bigs)
	
	print actual_total
	

	
	scale_factor_2 = (numpy.sum(bigs) * set_total) / numpy.sum(ys)
	
	scaled2 = bigs / (scale_factor_2 * ys)
	
	
	print 'scaled 2 sum', numpy.sum(scaled2)
	
	
	scaled1 = bigs * ((set_total + num_res) / actual_total)
	
	scaled = scaled1 + 1
	
	sum_scaled = numpy.sum(scaled)
	
	rounded_scaled = numpy.round(scaled)
	
	sum_rounded = numpy.sum(rounded_scaled)
	
	print 'sum scaled:', sum_scaled
	print 'sum rounded scaled', sum_rounded
	
	
	
	while numpy.sum(rounded_scaled) - set_total != 0:
		ins_res = numpy.sum(rounded_scaled) - set_total
		curr_ind = numpy.random.randint(num_res)
		if ins_res > 0:
			rounded_scaled[curr_ind] += -1
		elif ins_res < 0 and rounded_scaled[curr_ind]>1:
			rounded_scaled[curr_ind] += 1
	
	
	
	
	x = rounded_scaled

	x.sort()
	scaled.sort()
	bigs.sort()
	scaled2.sort()

	print numpy.sum(x)
	print x[:100]
	plt.semilogy(x)
	plt.semilogy(bigs)
	plt.semilogy(scaled2)
	plt.show()


	print numpy.polyfit(range(len(bigs)), numpy.log10(bigs), 1)

	



def mydistlog_linear(number_of_results, total_sum, m, c):
	
	
	base_xs = numpy.random.uniform(0, number_of_results, number_of_results)
	
	print base_xs[:100]
	
	
	ys_linearised = (m * base_xs) + c
	
	print ys_linearised[:100]
	
	ys_big = numpy.power(10, ys_linearised)
	
	print ys_big [:100]
	
	
	ys_scaled = ys_big * (float(total_sum) / float(numpy.sum(ys_big)))
	
	print ys_scaled[:100]
	
	
	print numpy.sum(ys_scaled)
	
	
	plt.plot(sorted(ys_scaled))
	plt.show()
	
	exit()
	
	
	
	
	return ys_scaled








def log_linear(number_of_results, total_sum, m, c):

	#and randomise the order
	#need to sort this with integer rounding.
	
	
	base_xs = numpy.random.uniform(0, number_of_results, number_of_results)
	
	#base_xs = numpy.array(range(number_of_results))
	
	
	#print base_xs[:100]
	
	
	ys_linearised = (m * base_xs) + c
	
	
	
	

	
	#print ys_linearised[:100]
	
	#exit()
	
	
	ys_big = numpy.power(10, ys_linearised)
	
	
	ys_scaled = ys_big * (float(numpy.sum(ys_big)) / float(numpy.log10(total_sum)))
	
	
	#print ys_linearised[:100]
	

	
	print numpy.sum(ys_scaled)
	
	
	a = numpy.polyfit(numpy.array(range(len(ys_big))), numpy.log10(sorted(ys_linearised)), deg=1)
	
	print a
	#print b
	#print c
	
	

	
	plt.plot(sorted(ys_scaled))
	plt.show()
	
	exit()
	
	return ys_scaled
	
	"""
	
	
	
)
	
	print ys_scaled
	
	
	
	
	rounded_scaled = numpy.round(ys_scaled)
	
	
	
	print rounded_scaled[:100]

	


	while numpy.sum(ys_scaled) - e != 0:
		ins_res = sum(ins_by_node) - e
		curr_ind = numpy.random.randint(n)
		if ins_res > 0:
			ins_by_node[curr_ind] += -1
		elif ins_res < 0:
			ins_by_node[curr_ind] += 1
	
	
	
	
	
	
	print numpy.sum(ys_scaled)
	
	return rounded_scaled



"""


def dep_ind_2(nodecount, edgecount):
	#check edge count makes sense
	
	ins_by_node = UUnifast_int(nodecount, edgecount)
	outs_by_node = UUnifast_int(nodecount, edgecount)
	
	ins_sorted = sorted(ins_by_node)[::-1]
	outs_sorted = sorted(outs_by_node)
	
	print ins_sorted
	print outs_sorted

	deps = {i: [] for i in range(nodecount)}
	
	print deps

	ind = nodecount-1
	while sum(ins_sorted) > 0:
		for i in range(ins_sorted[ind]):
			possible_ins
		ind = ind - 1



def dep_ind_3(nodecount, edgecount):
	#check edge count makes sense
	maxedges = nodecount * (nodecount - 1)
	
	if edgecount > maxedges:
		print "too many edges requested"
	if edgecount == maxedges:
		#complete graph - calc directly
		deplist = [range(i) for i in range(nodecount)]#[::-1]
		#sanity check topologically sorted
		for i in range(len(deplist)):
			if len(deplist[i]) > 1:
				if max(deplist[i]) >= i:
					print "not topologically sorted"
					for i in range(len(deplist)):
						print i, deplist[i]
					exit()
		return deplist
	
	#higher loop to catch really impossible distribs
	iter1 = 0
	fin1 = False
	
	
	while (not fin1) and (iter1 < 10):
		iter1 += 1
		fin1 = True
		
		ins_by_node = sorted(UUnifast_int(nodecount-1, edgecount) + [0])
		outs_by_node = sorted(UUnifast_int(nodecount-1, edgecount)  + [0])[::-1]
		
		#iter control
		finished = False
		itercount = 0
		
		#inner loop to try many different allocations. random isn't always perfect
		while (not finished) and itercount <= 40:
			finished = True
			outcounts = {x: outs_by_node[x] for x in range(nodecount)}
			incounts = {x: ins_by_node[x] for x in range(nodecount)}
			
			edges = []
			itercount += 1
			
			while len(edges) < edgecount:
				largest_node_needing_an_in = max([x[0] for x in incounts.items() if x[1] > 0])
				possible_outs = [x[0] for x in outcounts.items() if (x[1] > 0) and (x[0] < largest_node_needing_an_in)]
				
				#all_new_edges = set([(p, largest_node_needing_an_in) for p in possible_outs])
				#possible_edges = all_new_edges - edges
				
				
				possible_edges = [(p, largest_node_needing_an_in) for p in possible_outs if (p, largest_node_needing_an_in) not in edges]
				
				#print sorted(list(ps))
				#print possible_edges
				#exit()
				
				if len(possible_edges) == 0:
					finished = False
					break
				else:
					new_edge = random.choice(list(possible_edges))
					outcounts[new_edge[0]] -= 1
					incounts[new_edge[1]] -= 1
					edges.append(new_edge)
		
		if len(edges) < edgecount:
			fin1 = False

	if len(edges) < edgecount:
		print "no edge allocation found even after 40 tries with 10 distribs.\nare the input values sane?"
		print 'nodes requested', nodecount
		print 'edges requested', edgecount
		exit()


	deplist = [[] for i in range(nodecount)]
	for e in edges:
		deplist[e[1]].append(e[0])
	
	"""#sanity check topologically sorted
	for i in range(len(deplist)):
		if len(deplist[i]) > 1:
			if max(deplist[i]) >= i:
				print "not topologically sorted"
				for i in range(len(deplist)):
					print i, deplist[i]
				exit()
	print 'deplist', deplist"""
	#print iter1 * itercount
	return deplist
"""

def dependencies_with_industrial_degree_distribution(nodecount, edgecount):
	
	#check edge count makes sense
	maxedges = nodecount * (nodecount - 1)
	
	if edgecount > maxedges:
		print "too many edges requested"
	if edgecount == maxedges:
		#complete graph - calc directly
		deplist = [range(i) for i in range(nodecount)][::-1]
		return deplist

	#higher loop to catch really impossible distribs
	iter1 = 0
	fin1 = False


	while (not fin1) and (iter1 < 10):
		iter1 += 1
		fin1 = True

		#can lose these later
		ins_by_node = numpy.array(sorted(UUnifast_int(nodecount-1, edgecount) + [0]))
		outs_by_node = numpy.array(sorted(UUnifast_int(nodecount-1, edgecount)  + [0])[::-1])
		
		indices = numpy.array(range(nodecount))
		
		#iter control
		finished = False
		itercount = 0
		
		#inner loop to try many different allocations. random isn't always perfect
		while (not finished) and itercount <= 1000:
			finished = True
			#outcounts = {x: outs_by_node[x] for x in range(nodecount)}
			#incounts = {x: ins_by_node[x] for x in range(nodecount)}
			
			ins_np = numpy.copy(ins_by_node)
			outs_np = numpy.copy(outs_by_node)

			edges = []
			itercount += 1
			
			while len(edges) < edgecount:
				largest_node_needing_an_in = numpy.max(indices[ins_np > 0])
				
				#lin = max([x[0] for x in incounts.items() if x[1] > 0])

				#print largest_node_needing_an_in, lin
				

				#po = [x[0] for x in outcounts.items() if (x[1] > 0) and (x[0] < largest_node_needing_an_in)]
				
				possible_outs = indices[numpy.logical_and(outs_np > 0, indices < largest_node_needing_an_in)]
				
				#print po
				#print possible_outs
				
				#exit()
				
				possible_edges = [(p, largest_node_needing_an_in) for p in possible_outs if (p, largest_node_needing_an_in) not in edges]
				
				
				if len(possible_edges) == 0:			
					finished = False
					break
				else:
					new_edge = random.choice(possible_edges)
					outs_np[new_edge[0]] -= 1
					ins_np[new_edge[1]] -= 1
					edges.append(new_edge)
						
		if len(edges) < edgecount:
			fin1 = False
			
	if len(edges) < edgecount:
		print "no edge allocation found even after 1000 tries with 10 distribs.\nare the input values sane?"
		print 'nodes requested', nodecount
		print 'edges requested', edgecount
		exit()

						
	deplist = [[] for i in range(nodecount)]
	for e in edges:
		deplist[e[1]].append(e[0])
	
	#sanity check topologically sorted
	for i in range(len(deplist)):
		if len(deplist[i]) > 1:
			if max(deplist[i]) >= i:
				print "not topologically sorted"
				for i in range(len(deplist)):
					print i, deplist[i]
				exit()

	#print iter1 * itercount
	return deplist
"""
"""
	
	#indices_sorted = numpy.argsort(outs_by_node)
	
	#print indices_sorted
	
	found = False
	itercount = 0
	
	while (not found) and (itercount < 10000):
		itercount += 1
		print 'itercount', itercount
		
		found = True
		ins_left = {x: ins_by_node[x] for x in range(nodecount)}
		
		deplist = [[] for i in range(nodecount)]
		
		print ins_left
		
		for i in range(nodecount):
			ind = nodecount - (i+1)
			possible_outs = [y[0] for y in ins_left.items() if (y[0] < ind) and (y[1] > 0)]
			
			out_count = outs_by_node[i]
			
			print out_count, len(possible_outs), possible_outs

			if len(possible_outs) == out_count:
				deplist[ind] = possible_outs
			elif len(possible_outs) > out_count:
				random.shuffle(possible_outs)
				deplist[ind] = possible_outs[:out_count]
			else:
				print "not enough counts left"
				found = False
	
					
			for d in deplist[ind]:
				ins_left[d] -= 1
					
			print 'selected', deplist[ind]

		print "\n\n"


		for i in range(len(deplist)):
			print i, deplist[i]
				
				

		final_edge_count = sum([len(d) for d in deplist])
		if final_edge_count != edgecount:
			print "edge count mismatch"
			print 'desired', edgecount, 'actual', final_edge_count
			exit()

		exit()
			

		
		deplist = [[] for i in range(nodecount)]
		for i in indices_sorted[::-1]:
			print i
			this_dep_count = outs_by_node[i]
			
			print this_dep_count
			
			possible_outs = [x[0] for x in ins_left.items() if x[1] > 0]
			
			print possible_outs
			
			#exit()
			
			if len(possible_outs) == this_dep_count:
				actual_outs = possible_outs
			elif len(possible_outs) > this_dep_count:
				#should really be numpy.random.choice, but isn't available in this version
				actual_outs = numpy.random.permutation(possible_outs)[:this_dep_count]
			else:
				found = False
				actual_outs = []
					
			deplist[i].extend(list(actual_outs))
			for a in actual_outs:
				ins_left[a] += -1

	
			
	if itercount == 10000:
		print "failed to generate", nodecount, edgecount
		return [[]] * nodecount
		#exit()
	
	
	deplist_right_order = deplist[::-1]
	

	#how is this returning non-topologigally sorted graphs
	#are they even cycle-free?




	#check no cycles?
		
	return deplist_right_order
	
"""


#def years_work(jobcount, taskcount, depstyle):






def log_linear_industrial_raw(num_res):
	
	m =  3.83 * (10 ** -5)
	c = 0.569
	
	bases = numpy.random.uniform(0, 134461, num_res)
	ys = (m* bases) + c
	bigs = numpy.power(10, ys)
	
	bg_rounded = numpy.ceil(bigs)
	
	
	return bg_rounded


class random_curve_gen(object):
	def __init__(self, curves_file_path):
		filehandle = open(curves_file_path, 'r')
		
		self.possible_curve_names = [l.split(' ')[1] for l in filehandle if l[:5] == "Curve"]

		filehandle.close()
		
		if len(self.possible_curve_names) == 0:
			print "error: no curves found in file", curves_file_path
			exit()

	def random_curve_name(self):
		return random.choice(self.possible_curve_names)


class random_share_gen(object):
	def __init__(self, curves_file_path):
		filehandle = open(curves_file_path, 'r')
		
		possible_share_names = [(l.split(' ')[1], len(l.split(' ')[1].split('/')), int(l.split(' ')[2]))
								for l in filehandle if l[:5] == "share"]
		
		filehandle.close()

		maxdepth = max([x[1] for x in possible_share_names])
		
		bits = [[s[0]] * s[2] for s in possible_share_names if s[1] == maxdepth]
				
		self.shares_by_share = []
		
		for s in bits:
			self.shares_by_share.extend(s)
		
		#print self.shares_by_share
		
		if len(self.shares_by_share) == 0:
			print "error: no shares found in file", curves_file_path
			exit()
	
	def random_share_name(self):
		return random.choice(self.shares_by_share)







#def random_curve_from_file(in_file, )



class job_printer(object):
	def __init__(self, outfile):
		self.taskcounter = 1
		self.jobcounter = 1
	
		self.outfilehandle = open(outfile, 'w')
		
		
		self.jobsubmits = jsc2(5000, 100)
		self.next_time = 0
	


	def print_job(self, tcount, task_cores, task_execs, task_deps, task_kinds, value_curve, share_path):
		
		job_line_bits = ["Job j" + str(self.jobcounter),
						 "-3", #synthetic workloads don't have preset submit times. they are generated automatically relative to utilisation while sim is running. negative=invalid.
						 share_path,
						 value_curve]
		
		job_line = ' '.join(job_line_bits)
		
		
		self.outfilehandle.write(job_line)
		self.jobcounter += 1
		
		tbase = self.taskcounter
		
		for t in xrange(tcount):
			
			
			task_dep_bits = ["t" + str(i+tbase) for i in task_deps[t]]
			taskbit = ','.join(task_dep_bits)
			
			
			task_line_bits = ["Task t" + str(self.taskcounter),
							  str(task_cores[t]),
							  str(int(task_execs[t])),
							  task_kinds[t],
							  taskbit]
			
			
			task_line = ' '.join(task_line_bits) + "\n"
			
			self.outfilehandle.write(task_line)
			self.taskcounter += 1

		self.outfilehandle.write("\n")


	def print_job_hash(self, tcount, task_cores, task_execs, task_deps, task_kinds, value_curve, share_path):

	
		if tcount > 1:
		
			#tcount = tcount+2
			#task_cores = [1] + task_cores + [1]
			#task_execs = [1] + task_execs + [1]

			#check if multiple sources
			tdepcnt = 0

			for t in task_deps:
				if t == []:
					tdepcnt += 1
		


			#if multiple sources
			if tdepcnt > 1:
					
				for x in range(len(task_deps)):
					for y in range(len(task_deps[x])):
						task_deps[x][y] += 1
						
					task_deps[x] = sorted(task_deps[x])
			

					
				tcount += 1
				for tid in range(len(task_deps)):
					if task_deps[tid] == []:
						task_deps[tid] = [0]
						
				task_deps = [[]] + task_deps
				task_cores = [1] + task_cores
				task_execs = [1] + task_execs
				

					
			
			
			
			#check if multiple sinks
			tocnt = {i : 0 for i in range(tcount)}# collections.defaultdict(lambda : 0)
			
			for t in task_deps:
				for td in t:
					tocnt[td] += 1
			
			todcnt = 0

			if len(tocnt.items()) != tcount:
				print "length mismatch"
				exit()

			for t in tocnt.items():
				if t[1] == 0:
					todcnt += 1

			#if multiple sinks
			if todcnt > 1:
				
			
				
				final_deps = []
				
				for t in tocnt.items():
					if t[1] == 0:
						final_deps = final_deps + [t[0]]
				
				tcount += 1
				task_cores = task_cores + [1]
				task_execs = task_execs + [1]
				
				if len(final_deps) < 2:
					print final_deps
					print tcount
					print task_cores
					print task_execs
					print 'huh'
					exit()
				
				task_deps = task_deps + [final_deps]
			

		
		if tcount != len(task_deps):
			print tcount
			print len(task_deps)
			print task_deps
			print "error, task dep length mismatch with task count"
		
		tbase = self.taskcounter
		

		
		for t in range(tcount):
			out_bits = []
			out_bits.append(str(self.jobcounter))
			out_bits.append(str(self.taskcounter))
			out_bits.append(str(tcount))
			out_bits.append(str(task_cores[t]))
			out_bits.append(str(task_execs[t]))
			out_bits.append(str(sum(task_execs)))
			#deps_list_short = t.dependencies[:17]
			

			deps_bits_list = []
			
			
			if t == 0 and len(task_deps[t]) > 0:
				print "error"
				print task_deps
				exit()
			
			for i in range(17):
			
				if i < len(task_deps[t]):
					deps_bits_list.append(str(tbase + task_deps[t][i]))
				else:
					deps_bits_list.append(-1)
			
			deps_string = '/'.join([str(id) for id in deps_bits_list])
			
			out_bits.append(str(self.next_time))
			out_bits.append(deps_string)
			
			#print out_bits
			
			out_string = '\t'.join(out_bits)
			self.outfilehandle.write(out_string + '\n')
			self.taskcounter += 1
		
		
		
		self.jobcounter += 1
		
		
		class jb(object):
			def __init__(self, tm):
				self.tm = tm
			def sum_of_task_exectimes(self):
				return self.tm
		
		
		job = jb(sum(task_execs))
		
		
		
		self.next_time = self.jobsubmits.next_activation_time_with_day_week_cycles(job, self.next_time)
		
							
							
		

	
	
	
	
	
	
	
	
	

	def close(self):
		self.outfilehandle.close()


def taskexecs_to_sum(jsum, count):

	if jsum == count:
		return [1] * count
	
	#if jsum == count + 1:
	#	return [1] * (count -1 ) + [2]
	
	if jsum < count+1:
		print "err", jsum, count
		exit()
	
	
	
	divs = numpy.array(random.sample(xrange(jsum+1), count+1))

	divs.sort()

	execs = divs[1:] - divs[:-1]

	for e in execs:
		if e == 0:
			print 'zero'
			exit()
	if len(execs) != count:
		print 'mismatch'
		exit()

	return execs


def tasks_for_job_exec(jexec, min_tasks_per_job, max_tasks_per_job):
	if jexec <= min_tasks_per_job+1:
		print "warning: impossible tasks for job exec", min_tasks_per_job, jexec
		exit()
	
	bigtasks = min(max_tasks_per_job, jexec)
	
	task_count = numpy.random.random_integers(min_tasks_per_job, bigtasks)
		
	return task_count


def cores_for_task(texec, valid_cores_per_task, hash_format = False):
	
	#print 'here'
	#print texec
	#print valid_cores_per_task
	
	possible_cores = [v for v in valid_cores_per_task if (v <= texec) and (v >= math.floor(math.log(texec)))]
	
	#print texec
	#print possible_cores
	
	if len(possible_cores) == 0:
		actual_cores = sorted(valid_cores_per_task)[0]
	else:
		actual_cores = random.choice(possible_cores)
	
	return int(math.ceil(texec / actual_cores)), actual_cores

def years_work(jobcount, min_tasks_per_job, max_tasks_per_job, valid_cores_per_task, kinds_weighted, depstyle, depprob, curvesfile, sharesfile, outfile, hash=False):

	jprinter = job_printer(outfile)
	
	jexecs = numpy.array(log_linear_industrial_raw(jobcount))
	
	job_task_count = [tasks_for_job_exec(j, min_tasks_per_job, max_tasks_per_job) for j in jexecs]


#integers(min_tasks_per_job, max_tasks_per_job, jobcount)
		
	curvegen = random_curve_gen(curvesfile)
	sharegen = random_share_gen(sharesfile)
	

	
	#job_task_count[job_task_count > (jexecs )] = min_tasks_per_job
	
	
	
	#jexecs[job_task_count >= (jexecs / 2.0)] = job_task_count * 2
	
	
	#import matplotlib.pyplot as plt
	
	#plt.semilogy(sorted(jexecs))
	#plt.show()
	
	#exit()
	
	all_texecs = []
	all_tvolumes = []
	
	
	kinds = []
	for k in kinds_weighted:
		kinds.extend([k[0]] * int(k[1]))
	

	for j in xrange(jobcount):

		jc = job_task_count[j]
		
		#print 'task count', jc
		#print 'exectime', jexecs[j]
		
		
		#if jc >= jexecs[j] / 2.0:
		#	jexecs[j] = jc * 2
	
		
		task_execs_base = taskexecs_to_sum(int(jexecs[j]), int(jc))
		
		
		task_execs = []
		task_cores = []
		
		for t in task_execs_base:
			e, c = cores_for_task(t, valid_cores_per_task)
		
			task_execs.append(e)
			task_cores.append(c)
			all_tvolumes.append(e*c)
		


		#print task_execs
		#print task_cores
		all_texecs.extend(task_execs)
		#exit()
		
		
		#task_cores = [random.choice(valid_cores_per_task) for t in xrange(jc)]
		
		#task_execs = numpy.ceil() / numpy.array(task_cores))
		
		task_kinds = [random.choice(kinds) for t in xrange(jc)]
									 
		max_edges = jc * (jc - 1)
		edgecount = math.ceil((max_edges * depprob) * (1.0 / math.log10(max_edges)))
		
		if depstyle == 'industrial':
			task_deps  = dep_ind_3(jc, edgecount)
		else:
			print "error - need to define the other dep kinds"
			exit()

		job_value_curve = curvegen.random_curve_name()
		job_fair_share_path = sharegen.random_share_name()
		
		if not hash:
			jprinter.print_job(jc, task_cores, task_execs, task_deps, task_kinds, job_value_curve, job_fair_share_path)
		else:
			jprinter.print_job_hash(jc, task_cores, task_execs, task_deps, task_kinds, job_value_curve, job_fair_share_path)
							
	jprinter.close()

#	import matplotlib.pyplot as plt

#	plt.semilogy(sorted(all_tvolumes))
#	plt.show()
#	exit()



def make_file(id):
	years_work(20000,\
			   2, \
			   15,\
			   [1,12,24,36,64,96,128],\
			   [('Kind1',80), ('Kind2', 20)],\
			   'industrial',\
			   0.3,\
			   'big_curves_file.txt',\
			   'fairshares1.txt',\
			   '/Users/andie/Desktop/w2/workload' + str(id) + '.txt',
			   hash=False)
	return True



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




if __name__ == '__main__':

	#make_file(1)

	p = Pool()
	result = p.map(make_file, range(30))
	#make_file(1)
	
	#dep_ind_2(12,8)

	#deplist =  dependencies_with_industrial_degree_distribution(12,20)

	
	#for i in range(len(deplist)):
	#	print i, deplist[i]
	

	#for i in range(30):
	#cProfile.run("

				# ")
#'+str(i+1) + '
	





	"""
	#print dependencies_with_industrial_degree_distribution(12,30)
	#x = mydistlog_linear(10000, 100000000, (3.83 * 10 ** -5), 0.569)
	#print x
	#plt.plot(sorted(x))
	#plt.semilogy(sorted(x))
	#plt.show()

	#log_linear_2()
	#log_linear_raw()

	
	#print "Job j1 0 /root/none"
	#print "Task t1 1 100 Kind1"
	
	#for i in range(4000):
	#	print ""
	#	print "Job j"+ str(i+2), str(i), "/root/none"
	#	print "Task t"+ str(i+2)+ " 1 2 Kind1"


	"""


















