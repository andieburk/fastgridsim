import numpy
import multiprocessing
import matplotlib.pyplot as plt

print "loading"

class job(object):
	def __init__(self, name, lines):
		self.name = name
		self.lines = lines
		self.dependency_list = []
		self.chainval = 1
		
	def add_dependency(self, id):
		#print "adding dependency to", self.name, "on", id
		self.dependency_list.append(id)
		#print self.dependency_list
	
	def update_longest_chain(self, chainval):
		if chainval > self.chainval:
			self.chainval = chainval
	
	def print_self(self):
		print self.name
		print self.lines
		print self.dependency_list

input_real_filename = "/Volumes/NO NAME/anonymised simulation inputs/whole/anon_workload_whole.txt"

output_schedule_filename = "output_schedule_real_workload_whole_platform_sched.csv"

output_with_dependencies_filename = "real_with_interpolated_dependencies.csv"

inputfile = open(input_real_filename, "r")

lines = inputfile.readlines()

print len(lines)

jobnames = []
shares = []
submits = []
starts = []
finishes = []

jobnames_to_lines = {}

chain_length = []



for lineid in range(len(lines)):
	if lines[lineid][0:3] == "Job":
		jbits = lines[lineid].split(" ")
		tbits = lines[lineid + 1].split(" ")
		
		name = jbits[1]
		share_path = jbits[3]
		submit_time = jbits[2]
		start_time = tbits[5]
		finish_time = tbits[6]
		kind = tbits[4]
		
		nj = job(name, [jbits, tbits])
		jobnames_to_lines[name] = nj
		
		jobnames.append(name)
		submits.append(numpy.int32(submit_time))
		starts.append(numpy.int32(start_time))
		finishes.append(numpy.int32(finish_time))
		shares.append(numpy.int64(hash(share_path)))

inputfile.close()



shares_array = numpy.array(shares)
submits_array =  numpy.array(submits)
starts_array = numpy.array(starts)
finishes_array = numpy.array(finishes)

print len(jobnames)
print len(starts_array)
print len(finishes_array)
print len(shares_array)





d = []

searching_for_peak = True
analysing_matches = False

if searching_for_peak:
	min_value = numpy.int32(0)
	max_value = numpy.int32(300)
else:
	min_value = numpy.int32(10)
	max_value = numpy.int32(180)

lite = False

if lite:
	max_range = 10000
else:
	max_range = len(finishes_array)








def mini_diff_array_for_id(id):
	
	
	
	#print "one"
	source_name = jobnames[id]
	
	if finishes_array[id] <= starts_array[id]:
		print jobnames[id], "did not execute for any time at all, or was negative.\n started at", starts_array[id], "finished", finishes_array[id]
	
	#print "two"
	pi = submits_array[id] - finishes_array
	
	pi_big_enough = pi >= numpy.int32(min_value)
	pi_small_enough = pi <= numpy.int32(max_value)
	
	#print "three"
	
	#print len(shares_array), id, len(finishes_array)
	
	shares_match = shares_array[id] == shares_array

	
	
	#xcnt = numpy.count_nonzero(shares_match)
	#if xcnt > 0:
	#	print xcnt
	
	#print "four"
	p_one = numpy.logical_and(pi_big_enough, pi_small_enough)
	
	
	
	#print "five"
	pi_filter = numpy.logical_and(p_one, shares_match)
	
	#print numpy.count_nonzero(p_one) - numpy.count_nonzero(pi_filter)
		
	
	#print pi_filter[1:10]
	
	
	if searching_for_peak:
		pi_small = numpy.compress(pi_filter, pi)
		return pi_small
	
	elif analysing_matches:
		
		return numpy.count_nonzero(pi_filter)
		
		#poss_dependent_ids = numpy.compress(pi_filter, jobnames)
		
		#return poss_dependent_ids
		
		#if len(poss_dependent_ids) > 0:
	
			#print "job", jobnames[id], "hass poss dependents", poss_dependent_ids
		#	return (jobnames[id], poss_dependent_ids) 
		#else:
		#	return None
	else:
		poss_dependent_ids = numpy.compress(pi_filter, jobnames)
		
		any_deps = numpy.bool(len(poss_dependent_ids) > 0)
		
		#if any_deps:
		#	jobnames_to_lines[jobnames[id]].print_self()
		#	for d in poss_dependent_ids:
		#		jobnames_to_lines[d].print_self()
		
		print "finished", id
		
		return (any_deps, source_name, poss_dependent_ids)

p = multiprocessing.Pool()

print "searching"



#shares_match = shares_array[0] == shares_array

#print shares_match[1:1000]

#exit()







if searching_for_peak:

	all_minidiffs = p.map(mini_diff_array_for_id, range(max_range))

	newarr = numpy.concatenate(all_minidiffs)

	a,b = numpy.histogram(newarr, 720)


	print len(a), len(b)
	#print a, b

	#for id in range(len(a)):
	#	if a[id] > 52000:
	#		print b[id+1]
			

	plt.scatter(b[1:], a)

	plt.show()

elif analysing_matches:
	
	all_poss = p.map(mini_diff_array_for_id, range(max_range))
	#all_poss_filt = [x for x in all_poss if x != None]
	
	#all_poss_deps = [x[1] for x in all_poss_filt]
	
	#print max(all_poss)
	
	apd_arr = numpy.array([numpy.int32(x) for x in all_poss if x > 0])# and x < 100])
	
	#print numpy.max(apd_arr)
	
	a,b = numpy.histogram(apd_arr, 180)
	
	plt.scatter(b[1:], a)

	plt.show()

else:
	all_poss = p.map(mini_diff_array_for_id, range(max_range))
	
	new_deps = [d for d in all_poss if d[0]]
	
	def depcounter(jobid, seen_before):
		#print jobid
		
		if len(jobnames_to_lines[jobid].dependency_list) == 0:
			return 1
		else:
			new_dep_list_to_scan = set(jobnames_to_lines[jobid].dependency_list)
			
			not_seen_before = new_dep_list_to_scan - seen_before
			next_level_seen = new_dep_list_to_scan | seen_before

			return max([depcounter(jid, next_level_seen) for jid in not_seen_before])+1
	
	print len(new_deps)
	#print new_deps[0:40]

	
	
	for d in new_deps:
		for newdepid in d[2]:
			#print "adding", d[1], newdepid
			jobnames_to_lines[d[1]].add_dependency(newdepid)
	
	max_depcount = 0
	
	
	depcounts = numpy.array([numpy.int16(depcounter(d[1], set([]))) for d in new_deps])
	
	#for d in new_deps:
	#	nc = depcounter(d[1])
	#	if nc > max_depcount:
	#		max_depcount = nc
	
	print "max dependency depth", numpy.max(depcounts)
	
	
	a,b = numpy.histogram(depcounts, 100)
	
	plt.scatter(b[1:], a)

	plt.show()
	
	
	#print numpy.histogram(depcounts)
	
	
	

	
	#print len(new_deps)
	#for d in new_deps:
	#	print d
	
	
	print "finished."	
	
	
	
	
	
