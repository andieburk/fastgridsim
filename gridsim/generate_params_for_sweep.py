#industrial workload parameters gen
import os
import itertools

def industrial_workloads_compare_schedulers(invariant_dict, scheduler_list, parameter_file_output_folder, param_file_base_name):
	id = 1
	for s in scheduler_list:
		newdict = invariant_dict.copy()
		if s == "fair_share":
			newdict["router_orderer"] = "fifo_job"
			newdict["cluster_orderer"] = s
			newdict["fairshare_enabled"] = True
		else:
			newdict["router_orderer"] = "fifo_job"
			newdict["cluster_orderer"] = s
			newdict["fairshare_enabled"] = False
		
		outfilename = parameter_file_output_folder + "/" + param_file_base_name + s + ".txt"
		
		output_dict_to_params_file(newdict, outfilename)
		
		id += 1


def compare_n_params(invariant_dict, input_workloads, utilisations, netcosts, scheduler_list, parameter_file_output_folder, param_file_base_name):
	for n in netcosts:
		for s in scheduler_list:
			for u in utilisations:
				for w in input_workloads:
					newdict = invariant_dict.copy()
					if s == "fair_share":
						newdict["router_orderer"] = "fifo_job"
						newdict["cluster_orderer"] = s
						newdict["fairshare_enabled"] = True
					else:
						newdict["router_orderer"] = "fifo_job"
						newdict["cluster_orderer"] = s
						newdict["fairshare_enabled"] = False				
					
					newdict["target_utilisation"] = u
					newdict["workload_filename"]  = w
					newdict["communication_to_computation_ratio"] = n
					
					workload_simple_filename = w.split("/")[-1]
					
					outfilename = parameter_file_output_folder + "/" + param_file_base_name + s + "_" + str(u) + "_" + str(n) + "_" +workload_simple_filename
			
					output_dict_to_params_file(newdict, outfilename)
				
	
	

def generate_sweep_from_sweep_ranges(self, invariants, permutations, multi_values):
	
	invariants = {'a':1, 'b':2, 'c':3}
	permutations = {'d': [4,5,6], 'e':[7]}
	multi_values = {'f':[8,9], 'g':[10,11]}
	
	
	
	if len(multi_values.items()) != 0:
		
		multi_lens = [len(m) for m in multi_values.values()]
		
		for m1 in multi_lens:
			if m1 != multi_lens[0]:
				print "error, mismatched input lengths in multi-values"

	bits_to_permute = [(p[0],q) for q in p[1] for p in permutations.items()]
	
	print bits_to_permute



		
	output_dict = invariants.copy()
	
	
	
	
	
	
	yield output_dict





		
def output_dict_to_params_file(dict_to_output, filename):
	f = open(filename, 'w')
	for i in dict_to_output.items():
		#print type(i[1])
		if type(i[1]) == type('string'):
			oneline = '"%s" : "%s"\n' % (i[0],i[1])
		else:
			oneline = '"%s" : %s\n' %(i[0],i[1])
		f.write(oneline)
	f.close()
	#print "exported parameter file to", filename

def get_invariant_dict_ind_workload():
	invariant_dict = {
		"platform_filename":"/Volumes/NO NAME/anonymised simulation inputs/hl/anon_platform_hl.txt",
		"workload_filename":"/Volumes/NO NAME/anonymised simulation inputs/hl/anon_workload_hl.txt",
		"debugging_on" : False,
	#	"cluster_orderer" : "projected_slr",
		"cluster_allocator" : "first_free",
	#	"router_orderer": "projected_slr",
		"router_allocator": "first_free",
		"target_utilisation": 100,
		"staggered_release":True,
		"use_supplied_submit_times":True,
		"simulation_max_time":10000000000000000000,
		"faster_less_accurate_charting": True,
		"faster_charting_factor":1000,
		"visualisation_enabled": False,
		"visualise_each_task" : False,
		"fairshare_tree_filename" : "/Volumes/NO NAME/anonymised simulation inputs/hl/anon_fairshare_tree_whole_new.txt",
		"print_metrics_by_job": False,
		"print_global_metrics": True,
		"max_job_count" : 100000,
		"output_schedule_to_file" : False,
		"schedule_output_filename" : "depprob4.csv",
	#	"fairshare_enabled" : False
			}
	return invariant_dict

def get_invariant_dict_synthetics():
	invariant_dict = {
		"platform_filename":"/Users/andy/Documents/Work/gridsimxcode/GridSim/gridsim/platform_1_3000_1_1000.txt",
	#	"workload_filename":"/Volumes/NO NAME/anonymised simulation inputs/hl/anon_workload_hl.txt",
		"debugging_on" : False,
	#	"cluster_orderer" : "projected_slr",
		"cluster_allocator" : "first_free",
		"router_orderer": "fifo_job",
		"router_allocator": "load_balancing_allocator",
	#	"target_utilisation": 100,
		"staggered_release":True,
		"use_supplied_submit_times":False,
		"simulation_max_time":10000000000000000000,
		"faster_less_accurate_charting": True,
		"faster_charting_factor":1000,
		"visualisation_enabled": False,
		"visualise_each_task" : False,
		"fairshare_tree_filename" : "/Users/andy/Documents/Work/gridsimxcode/GridSim/gridsim/fairshares2.txt",
		"print_metrics_by_job": False,
		"print_global_metrics": True,
		"max_job_count" : 100000,
		"output_schedule_to_file" : False,
		"schedule_output_filename" : "depprob4.csv",
		"cumulative_completion_base" : 10000000000,
		"networking_enabled" : True,
		"network_delay_level_power" : 1,
	#	"communication_to_computation_ratio" : 0.2,
		"proportion_of_kind2" : 0.2
	#	"fairshare_enabled" : False
			}
	return invariant_dict	
	
	

def generate_ind_workload_params():
	inv_dict = get_invariant_dict_ind_workload()
	scheduler_list = ["random", "fifo_task", "fifo_job", "fair_share", "projected_slr"]
	output_folder = "/Users/andy/Documents/Work/journal_paper_results/ind_wld_params_2"
	param_file_base_name = "params_"
	industrial_workloads_compare_schedulers(inv_dict, scheduler_list, output_folder, param_file_base_name)




def generate_synthetics_params():
	inv_dict = get_invariant_dict_synthetics()
	scheduler_list = ["shortest_remaining_first", "longest_remaining_first", "random", "fifo_task", "fifo_job", "fair_share", "projected_slr"]
	input_folder_base = "/Users/andy/Documents/Work/workloads_march_12/synthetics"
	
	
	filename_list = []
	
	for path, foldernames, filenames in os.walk(input_folder_base):
		for name in filenames:
			if name[0] != ".":
				filename_list.append(path + "/" + name)
	
	utilisations = [80, 90, 100, 110, 120]
	netcosts = [0.1, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
	
	output_folder = "/Users/andy/Documents/Work/netcosts_massive_sweep"
	param_file_base_name = "params_"
	compare_n_params(inv_dict, filename_list, utilisations, netcosts, scheduler_list, output_folder, param_file_base_name)
	


def generate_fairshare_params():
	inv_dict = get_invariant_dict_synthetics()
	scheduler_list = ["fair_share"]
	input_folder_base = "/Users/andy/Documents/Work/workloads_march_12/synthetics"
	
	filename_list = []
	
	for path, foldernames, filenames in os.walk(input_folder_base):
		for name in filenames:
			if name[0] != ".":
				filename_list.append(path + "/" + name)
	
	utilisations = [80, 90, 100, 110, 120]
	
	output_folder = "/Users/andy/Documents/Work/journal_paper_results/fairshare_params"
	param_file_base_name = "params_"
	compare_n_params(inv_dict, filename_list, utilisations, scheduler_list, output_folder, param_file_base_name)









def generate_synthetics_params_2():
	inv_dict = get_invariant_dict_synthetics()
	scheduler_list = ["identity", "fifo_job", "projected_slr"]
	input_folder_base = "/Users/andy/Documents/Work/workloads_march_12/synthetics"
	
	
	filename_list = []
	
	for path, foldernames, filenames in os.walk(input_folder_base):
		for name in filenames:
			if name[0] != ".":
				filename_list.append(path + "/" + name)
	
	utilisations = [90, 110]
	
	output_folder = "/Users/andy/Documents/Work/journal_paper_results/main_params_2"
	param_file_base_name = "params_"
	compare_n_params(inv_dict, filename_list, utilisations, scheduler_list, output_folder, param_file_base_name)



#def compare_n_params(invariant_dict, input_workloads, utilisations, scheduler_list, parameter_file_output_folder, param_file_base_name):









if __name__ == "__main__":
	print "generating parameter files"
	generate_synthetics_params()
	print "finished"



"""



"platform_filename":"platform1000cores.plat"
"workload_filename":"/Users/andy/Documents/Work/workloads_march_12/power_jobs_chains_blocks/jobs_chains_blocks_1.txt"
"debugging_on" : False
"cluster_orderer" : "projected_slr"
"cluster_allocator" : "first_free"
"router_orderer": "projected_slr"
"router_allocator": "first_free"
"target_utilisation": 100
"staggered_release":True
"use_supplied_submit_times":False
"simulation_max_time":10000000000000000000
"faster_less_accurate_charting": True
"faster_charting_factor":400
"visualisation_enabled": False
"visualise_each_task" : False
"fairshare_tree_filename" : "/Volumes/NO NAME/anonymised simulation inputs/hl/anon_fairshare_tree_whole.txt"
"print_metrics_by_job": False
"print_global_metrics": True
"max_job_count" : 1
"output_schedule_to_file" : False
"schedule_output_filename" : "depprob4.csv"
"fairshare_enabled" : False

"""