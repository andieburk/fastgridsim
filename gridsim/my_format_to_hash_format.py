"""
Convert Andrew's simulator workflow format into the format needed by Hashem
Andrew Burkimsher, June 2012 
"""

#Imports
import sys
import os

#constants of the indices in the input file
job_name_index = 1
submit_time_index = 2
fairshare_index = 3
taskname_index = 1
taskcores_index = 2
taskexec_index = 3
taskkind_index = 4
taskdeps_index = 5




#read and parse the input file


def read_file(input_file, output_file):
	f = open(input_file, 'r')
	
	o = open(output_file, 'w')
	
	curr_job_name = ""
	curr_job_submit = ""
	curr_fairshare = ""
	
	o.write("#job_num task_num cores_num exec_time dependencies\n")
	
	
	for line in f:
		if len(line) > 0:
			lsplit = line.split(" ")
			if lsplit[0] == "Job":
				curr_job_name = lsplit[job_name_index][1:]
				curr_submit_time = lsplit[submit_time_index]
				curr_fairshare = lsplit[fairshare_index]
		
			elif lsplit[0] == "Task":
				curr_task_name = lsplit[taskname_index][1:]
				curr_task_cores = lsplit[taskcores_index]
				curr_task_exec = lsplit[taskexec_index]
				curr_task_kind = lsplit[taskkind_index][4:]
				curr_task_deps = lsplit[taskdeps_index][:-1]
				
				if len(curr_task_deps) > 0:
					cdepsnums = [t[1:] for t in curr_task_deps.split(",")]
					curr_task_deps = ",".join(cdepsnums)
			
				
				#this next line is the bit you need to edit if the output format should change
				hash_form_array = [curr_job_name, curr_task_name, curr_task_cores, curr_task_exec, curr_task_deps] 
				
				outstr = ' '.join(hash_form_array)
				outstr = outstr + "\n"
				o.write(outstr)
	
	f.close()
	o.close()




if __name__ == "__main__":

	input_folder = "/Users/andy/Documents/Work/workloads_march_12/synthetics/uniform_jobs_independent/"
	output_folder = "/Users/andy/Documents/Work/workloads_march_12/synthetics_hash/uniform_jobs_independent/"
	
	all_in_source_dir = [f for f in os.listdir(input_folder) if f[0] != "." and f[-1] != "~"] #filter hidden or backup files
	
	
	for f in all_in_source_dir:

		input_file = input_folder + f
		output_file = output_folder + "hashf_" + f
		
		print "transforming", input_file, "in Andrew format into", output_file, "in Hashem format"
		
		read_file(input_file, output_file)
		
	print "transformations complete"
	
	exit()
	
	
	
	
	
