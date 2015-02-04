import numpy




def load_workload(path):
	
	filehandle = open(path, 'r')


	workload_dict = {}
	exec_list = []

	curr_job = None

	#jobcount = 1

	for line in filehandle:
		lsplit = line.split(' ')
		if lsplit[0] == "Job":
			curr_job = lsplit[1]
			workload_dict[curr_job] = 0
			exec_list.append(0)
			#jobcount += 1
		elif lsplit[0] == "Task":
			cores = int(lsplit[2])
			exec_time = int(lsplit[3])
			workload_dict[curr_job] += float(cores * exec_time) #float
			exec_list[-1] += float(cores * exec_time) #float


	#if jobcount != len(exec_list):
	#	print jobcount, len(exec_list)
	#	print 'read in error length mismatch'
	#	exit()

	filehandle.close()
	
	exec_array = numpy.array(exec_list)

	return workload_dict, exec_array






if __name__ == "__main__":

	l, c = load_workload("/Users/andie/Documents/Work/simulation run results used in thesis/workloads_june_2013/work2/ind_style_workload15.txt")

	b = l.items()

	print b[:20]

	print c[:20]


