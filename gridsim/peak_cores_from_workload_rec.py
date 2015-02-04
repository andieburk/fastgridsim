
import numpy


input_real_filename = "/Volumes/NO NAME/anonymised simulation inputs/hl/anon_workload_hl.txt"

output_schedule_filename = "output_schedule_real_workload_sched.csv"


inputfile = open(input_real_filename, "r")

lines = inputfile.readlines()

jobs = []


min_start = -1
max_finish = 0

print len(lines)

for lineid in range(len(lines)):
	if lines[lineid][0:3] == "Job":
		jbits = lines[lineid].split(" ")
		tbits = lines[lineid + 1].split(" ")
		name = jbits[1]
		submit_time = jbits[2]
		start_time = numpy.int64(tbits[5])
		finish_time = numpy.int64(tbits[6])
		cores = numpy.int16(int(tbits[2]))
		
		jobs.append([name, cores, start_time, finish_time])
		
		if min_start < 0 or min_start > start_time:
			min_start = start_time
		
		if max_finish < finish_time:
			max_finish = finish_time
		
		
		
		
		"""
		slr = float(int(finish_time) - int(submit_time)) / float(int(finish_time) - int(start_time))
		
		slrstr = str(slr)
		
		out_bits = []
				
		out_bits.append(name)
		out_bits.append(submit_time)
		out_bits.append(start_time)
		out_bits.append(finish_time)
		out_bits.append(slrstr)

		out_str = ','.join(out_bits)
		"""
		
		
inputfile.close()

timestep_core_change = []


for j in jobs:
	timestep_core_change.append((j[2], j[1]))
	timestep_core_change.append((j[3], -j[1]))

max_cores_running = 0
curr_cores_running = 0


for ts in sorted(timestep_core_change, key=lambda x: float(x[0]) + (float(x[1]) / 100000000)):
	curr_cores_running += ts[1]
	if curr_cores_running > max_cores_running:
		max_cores_running = curr_cores_running


print "max cores running", max_cores_running
	
	



















"""
this bit is much, much too slow to be practical. (n squared on a big, big N.)
def cores_running_at(time):
	return numpy.sum(numpy.array([j[1] for j in jobs if time >= j[2] and time < j[3]]))



max_cores_running = 0

for i in range(min_start, min_start+100):
	cores_at_time = cores_running_at(i)
	if cores_at_time > max_cores_running:
		max_cores_running = cores_at_time


print 'max cores running', max_cores_running
"""

print "done."
exit()



