#industrial workload parameters gen
import os

from permutes import generate_sweep_from_sweep_ranges

def workloads_from_folder(root_path):
	filename_list = []
	
	for path, foldernames, filenames in os.walk(root_path):
		for name in filenames:
			if name[0] != ".":
				filename_list.append(path + "/" + name)

	return filename_list







def generate_synthetics_params_new(workloads_path, output_folder):

	invariant_dict = {
		"platform_filename":"/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/platform_6_400_1_400.txt",
	#	"workload_filename":"/Volumes/NO NAME/anonymised simulation inputs/hl/anon_workload_hl.txt",
		"debugging_on" : False,
	#	"cluster_orderer" : "projected_slr",
		"cluster_allocator" : "first_free",
		"router_orderer": "fifo_job",
	#	"router_allocator": "load_balancing_allocator",
	#	"target_utilisation": 100,
		"staggered_release":True,
		"use_supplied_submit_times":False,
		"simulation_max_time":10000000000000000000,
		"faster_less_accurate_charting": True,
		"faster_charting_factor":1000,
		"visualisation_enabled": False,
		"visualise_each_task" : False,
		"fairshare_tree_filename" : "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/fairshares2.txt",
		"print_metrics_by_job": False,
		"print_global_metrics": True,
		"max_job_count" : 100000,
		"human_readable_metrics" : False,
		"output_schedule_to_file" : False,
		"schedule_output_filename" : "depprob4.csv",
		"cumulative_completion_base" : 10000000000,
		"networking_enabled" : True,
		"network_delay_level_power" : 2,
	#	"communication_to_computation_ratio" : 0.2,
		"proportion_of_kind2" : 0.2
	#	"exec_time_estimate_kind" : "exact"
	#	"exec_time_estimate_value" : 100
	#	"fairshare_enabled" : False
			}	
	
	filled_out_dicts = []
	
	baseline = False
	
	
	#workloads = {"workload_filename" : ['w1', 'w2']}
	
	#permutations
	workloads = {"workload_filename" : workloads_from_folder(workloads_path)}

	utilisations = {"target_utilisation" : [120]}
	
	network_costs = {"communication_to_computation_ratio" : [0.0, 0.2, 0.4, 0.6, 0.8]}
	#network_costs = {"communication_to_computation_ratio" : [0.2]}

	router_allocator = {"router_allocator" : ["random", "load_balancing_allocator"]}

	#multi-bits

	for baseline in [False, True]:


		if baseline:
			
			schedulers = [{"cluster_orderer" : ["random",
												"fifo_task",
												"fifo_job",
												"fair_share"],
												
						   "fairshare_enabled": [False,
												 False,
												 False,
												 True]
						   }]
						   
						   
			inac_estimates = [{"exec_time_estimate_kind" : ["exact"],
							 "exec_time_estimate_value" : [1]}]
				
		else:
		
			schedulers = [{"cluster_orderer" : ["projected_slr",
												"shortest_remaining_first",
												"longest_remaining_first"],
												
						   "fairshare_enabled": [False,
												 False,
												 False]
						   }]
		
		
		
			inac_estimates = [ {"exec_time_estimate_kind" : ["exact"],
							   "exec_time_estimate_value" : [1]}#,
		#					   {"exec_time_estimate_kind" : ["normal_jitter"] * 13,
		#						"exec_time_estimate_value" : [10,20,40,60,80,100,200,1000,10000,100000,1000000,10000000,100000000]},
		#					   {"exec_time_estimate_kind" : ["log_round"] * 12,
		#					   "exec_time_estimate_value" : [2,3,4,5,10,20,100,1000,10000,100000,1000000,10000000]},
							  ]
		
		all_multis = schedulers + inac_estimates
		#print len(all_multis), all_multis
		
		output_dicts = generate_sweep_from_sweep_ranges(
										invariant_dict,
										dict(workloads.items() +  utilisations.items() + network_costs.items() + router_allocator.items()),
										schedulers + inac_estimates,
										False)
		
		all_new_dicts = [i for i in output_dicts]
		print len(all_new_dicts)
		filled_out_dicts.extend(all_new_dicts)
		print len(filled_out_dicts)
		
	
	print len(filled_out_dicts)

	
	#optimise by stripping out the estimated times for the schedulers it doesn't affect
	"""filled_filtered = []
	
	non_estimate_using_schedulers = set(["random", "fifo_task", "fifo_job", "fair_share"])
	
	filtered_counter = 0
	
	for j in filled_out_dicts:
		#print j["cluster_orderer"]
		if j["cluster_orderer"] in non_estimate_using_schedulers:
			if j["exec_time_estimate_kind"] == "exact":
				filled_filtered.append(j)
			else:
				#print "not adding", j
				filtered_counter += 1
				pass #filter out
		else:
			filled_filtered.append(j)
	
	
	print filtered_counter, 'configurations filtered out for non-estimate-using-schedulers'
	
	
	print len(filled_filtered)
	
	#for i in filled_filtered:
	#	print i
	"""
	params_id = 0

	
	
	for d in filled_out_dicts:
		params_id += 1
		output_dict_to_params_file(d, output_folder + '/params_' + str(params_id) + '.txt')
	



















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


def compare_n_params(invariant_dict, input_workloads, utilisations, netcosts, scheduler_list, parameter_file_output_folder, param_file_base_name, innaccurate_est_kinds = None, innaccurate_est_values = []):
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
					
					if innaccurate_est_kinds != None:
						
						if innaccurate_est_values == None or len(innaccurate_est_values) != len(innaccurate_est_kinds):
							print "error in values supplied for innaccurate estimations"
							print innaccurate_est_kinds
							print innaccurate_est_values
							exit()
						
						for kid in range(len(innaccurate_est_kinds)):
							
							newdict["exec_time_estimate_kind"] = innaccurate_est_kinds[kid]
							
							for val in innaccurate_est_values[kid]:
								
								newdict["exec_time_estimate_value"] = val
								
								outfilename = parameter_file_output_folder + "/" + param_file_base_name + s + "_" + str(u) + "_" + str(n) + "_" + innaccurate_est_kinds[kid] + "_" + str(val) + "_" +workload_simple_filename
								
								output_dict_to_params_file(newdict, outfilename)
					
					else:
						
						outfilename = parameter_file_output_folder + "/" + param_file_base_name + s + "_" + str(u) + "_" + str(n) + "_" +workload_simple_filename
			
						output_dict_to_params_file(newdict, outfilename)
				
	
	





		
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
		"platform_filename":"/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/platform_1_3000_1_1000.txt",
	#	"workload_filename":"/Volumes/NO NAME/anonymised simulation inputs/hl/anon_workload_hl.txt",
		"debugging_on" : False,
	#	"cluster_orderer" : "projected_slr",
		"cluster_allocator" : "first_free",
		"router_orderer": "fifo_job",
		"router_allocator": "random",
	#	"target_utilisation": 100,
		"staggered_release":True,
		"use_supplied_submit_times":False,
		"simulation_max_time":10000000000000000000,
		"faster_less_accurate_charting": True,
		"faster_charting_factor":1000,
		"visualisation_enabled": False,
		"visualise_each_task" : False,
		"fairshare_tree_filename" : "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/fairshares2.txt",
		"print_metrics_by_job": False,
		"print_global_metrics": True,
		"max_job_count" : 100000,
		"output_schedule_to_file" : False,
		"schedule_output_filename" : "depprob4.csv",
		"cumulative_completion_base" : 10000000000,
		"networking_enabled" : True,
		"network_delay_level_power" : 2,
	#	"communication_to_computation_ratio" : 0.2,
		"proportion_of_kind2" : 0.2,
	#	"fairshare_enabled" : False
		"exec_time_estimate_kind" : "exact",
		"exec_time_estimate_value" : 100
			}
	return invariant_dict

def get_invariant_dict_synthetics_innaccurate_estimates():
	invariant_dict = {
		"platform_filename":"/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/platform1000cores.plat",
	#	"workload_filename":"/Volumes/NO NAME/anonymised simulation inputs/hl/anon_workload_hl.txt",
		"debugging_on" : False,
	#	"cluster_orderer" : "projected_slr",
		"cluster_allocator" : "first_free",
		"router_orderer": "fifo_job",
		"router_allocator": "first_free",
	#	"target_utilisation": 100,
		"staggered_release":True,
		"use_supplied_submit_times":False,
		"simulation_max_time":10000000000000000000,
		"faster_less_accurate_charting": True,
		"faster_charting_factor":1000,
		"visualisation_enabled": False,
		"visualise_each_task" : False,
		"fairshare_tree_filename" : "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/fairshares2.txt",
		"print_metrics_by_job": False,
		"print_global_metrics": True,
		"max_job_count" : 100000,
		"output_schedule_to_file" : False,
		"schedule_output_filename" : "depprob4.csv",
		"cumulative_completion_base" : 10000000000,
		"networking_enabled" : False,
		"network_delay_level_power" : 2,
		"communication_to_computation_ratio" : 0.2,
		"proportion_of_kind2" : 0.0
	#	"exec_time_estimate_kind" : "exact"
	#	"exec_time_estimate_value" : 100
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
	input_folder_base = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/workloads_march_12/synthetics"
	
	#input_folder_base = "/Users/andy/Documents/Work/workloads_march_12/synthetics"
	
	#eventual_input_folder_base = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/workloads_march_12/synthetics"
	
	filename_list = []
	
	for path, foldernames, filenames in os.walk(input_folder_base):
		for name in filenames:
			if name[0] != ".":
				filename_list.append(path + "/" + name)
	
	utilisations = [80, 90, 100, 110, 120]
	netcosts = [0.1, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
	
	output_folder = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/network_costs_sweep"
	
	#output_folder = "/Users/andy/Desktop/netcosts_sweep"
	
	param_file_base_name = "params_"
	compare_n_params(inv_dict, filename_list, utilisations, netcosts, scheduler_list, output_folder, param_file_base_name)



def generate_synthetics_params_innacc_ests():
	inv_dict = get_invariant_dict_synthetics_innaccurate_estimates()
	scheduler_list = ["shortest_remaining_first", "longest_remaining_first", "random", "fifo_task", "fifo_job", "fair_share", "projected_slr"]
	input_folder_base = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/workloads_march_12/synthetics"
	
	#input_folder_base = "/Users/andy/Documents/Work/workloads_march_12/synthetics"
	
	#eventual_input_folder_base = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/workloads_march_12/synthetics"
	
	filename_list = []
	
	for path, foldernames, filenames in os.walk(input_folder_base):
		for name in filenames:
			if name[0] != ".":
				filename_list.append(path + "/" + name)
	
	utilisations = [80, 90, 100, 110, 120]
	netcosts = [1.0] # [0.1, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
	
	estimate_errors = ["exact", "normal_jitter", "log_round", "round"]
	error_values = [[100], [10,20,50,150,200], [2,5,10,20], [1000,10000,100000]]
	
	output_folder = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/inac_sweep"
	
	#output_folder = "/Users/andy/Desktop/inac_out"
	
	param_file_base_name = "params_"
	compare_n_params(inv_dict, filename_list, utilisations, netcosts, scheduler_list, output_folder, param_file_base_name,
	estimate_errors, error_values)
	

def gen_syn_params_both():
	inv_dict = get_invariant_dict_synthetics_innaccurate_estimates()
	scheduler_list = ["shortest_remaining_first", "longest_remaining_first", "random", "fifo_task", "fifo_job", "fair_share", "projected_slr"]
	input_folder_base = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/workloads_march_12/synthetics"
	
	#input_folder_base = "/Users/andy/Documents/Work/workloads_march_12/synthetics"
	
	#eventual_input_folder_base = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/workloads_march_12/synthetics"
	
	filename_list = []
	
	for path, foldernames, filenames in os.walk(input_folder_base):
		for name in filenames:
			if name[0] != ".":
				filename_list.append(path + "/" + name)
	
	utilisations = [80, 90, 100, 110, 120]
	netcosts = [1.0] # [0.1, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
	
	estimate_errors = ["exact", "normal_jitter", "log_round", "round"]
	error_values = [[100], [10,20,50,150,200], [2,5,10,20], [1000,10000,100000]]
	
	output_folder = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/inac_sweep"
	
	#output_folder = "/Users/andy/Desktop/inac_out"
	
	param_file_base_name = "params_"
	compare_n_params(inv_dict, filename_list, utilisations, netcosts, scheduler_list, output_folder, param_file_base_name,
	estimate_errors, error_values)








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
	#print "generating parameter files"
	#generate_synthetics_params_innacc_ests()
	#print "finished"
	#for f in workloads_from_folder("/Users/andy/Documents/Work/workloads_march_12/synthetics"):
	#	print f
	#print len(workloads_from_folder("/Users/andy/Documents/Work/workloads_march_12/synthetics"))

	#generate_synthetics_params_new("/Users/andy/Documents/Work/workloads_march_12/synthetics",
	#								"/Users/andy/Desktop/test_outs_1")

	generate_synthetics_params_new("/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/workloads_march_12/synthetics",
									"/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/net_ins")
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