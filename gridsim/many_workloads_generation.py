#generate lots of workloads

from pool_runner import *

wld_params = {
	"num_jobs" : "1",
	"total_exec_sum" : "100",
	"minchains": "1",
	"maxchains": "2",
	"minchainlength": "1",
	"maxchainlength": "2",
	"workload_kinds": ["dep_prob_0.3"],
	"minblocks" : "1",
	"maxblocks" : "8",
	"num_workloads": 1,
	"start_random_seed": 123456,
	"filename_root" : "/Users/andy/Documents/Work/workloads_march_12/",
	"filename_end" : ".txt"
	}

exec_strings = []

for k in wld_params["workload_kinds"]:
	for i in range(wld_params["num_workloads"]):
		output_file_name = wld_params["filename_root"] + k + "_" + str(i) + wld_params["filename_end"]
		if k == "dep_prob_0":
			dep_prob = "0.0"
			workload_structure = "1"
		if k == "dep_prob_0.3":
			dep_prob = "0.3"
			workload_structure = "3"
		if k == "linear_chains":
			dep_prob = "1.0"
			workload_structure = "1"
		if k == "independent_chains":
			workload_structure = "1"
		if k == "chain_of_independent_chains":
			workload_structure = "2"
		
		wld_params["start_random_seed"] = wld_params["start_random_seed"] + 1
		
		execstring = "python workload_generator_2.py --numjobs " + wld_params["num_jobs"] +\
												 " --totalexecsum " + wld_params["total_exec_sum"] +\
												 " --minchains " + wld_params["minchains"] +\
												 " --maxchains " + wld_params["maxchains"] +\
												 " --minchainlength " + wld_params["minchainlength"] +\
												 " --maxchainlength " + wld_params["maxchainlength"] +\
												 " --minblocks " + wld_params["minblocks"] +\
												 " --maxblocks " + wld_params["maxblocks"] +\
												 " --randomseed " + str(wld_params["start_random_seed"]) +\
												 " --outputfile " + output_file_name +\
												 " --depprob " + dep_prob +\
												 " --workloadstructure " + workload_structure
		exec_strings.append(execstring)
		
		
#print exec_strings
parallel_exec_strings(exec_strings)
print "execution of many_workloads_generation finished"


