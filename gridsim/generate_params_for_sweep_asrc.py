#industrial workload parameters gen
import os

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
		
		outfilename = parameter_file_output_folder + "/params_" + int(id) + ".txt"
		
		output_dict_to_params_file(newdict, outfilename)
		
		id += 1


def compare_n_params(invariant_dict, input_workloads, utilisations, netcosts, scheduler_list, parameter_file_output_folder, param_file_base_name, innaccurate_est_kinds = None, innaccurate_est_values = [], arrival_patterns = ["static_pattern"], timeouts_enabled = ["True"]):
	
	print len(input_workloads)
	
	
	
	number_of_sims = len(input_workloads) * len(utilisations) * len(netcosts) * len(scheduler_list) * len(innaccurate_est_kinds) * len(innaccurate_est_values) * len(arrival_patterns) * len(timeouts_enabled)
	
	print "outputting ", number_of_sims, "simulations"
	
	if number_of_sims > 75000:
		print "number of sims is over 75000, this will fail on the ASRC cluster. keep to below 75k"
	
	id_counter = 0
	
	for t in timeouts_enabled:
		for a in arrival_patterns:
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
							newdict["arrival_pattern"] = a
							newdict["timeouts_enabled"] = t
						
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
										
										outfilename = parameter_file_output_folder + "/" + "run_params_" + str(id_counter) + ".txt"
										id_counter += 1
										output_dict_to_params_file(newdict, outfilename)
							
							else:
								
								outfilename = parameter_file_output_folder + "/" + "run_params_" + str(id_counter) + ".txt"
								id_counter += 1
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




def get_invariant_dict_utilisation_study():
	invariant_dict = {
		"platform_filename":"/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/platform_real2.txt",
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
		"fairshare_tree_filename" : "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/fairshares1.txt",
		"print_metrics_by_job": False,
		"print_global_metrics": True,
		"max_job_count" : 12500,
		"output_schedule_to_file" : False,
		"schedule_output_filename" : "depprob4.csv",
		"cumulative_completion_base" : 2628000,
		"networking_enabled" : True,
		"network_delay_level_power" : 2,
		#"communication_to_computation_ratio" : 0.2,
		"proportion_of_kind2" : 0.0,
			#	"exec_time_estimate_kind" : "exact"
			#	"exec_time_estimate_value" : 100
			#	"fairshare_enabled" : False
		"pickle_output_path" : "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/netfinal_pickles/",
			#"metrics_out_formats" : ["text_human", "pickle"]
		"metrics_out_formats" : ["pickle"],
		"value_enabled" : True,
		"curves_filename" : "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/big_curves_file.txt",
		"job_exec_timeout_factor" : 10,
		"random_seed" : 123456,
		"print_schedule" : False,
		#"arrival_pattern" : "static_pattern"
		#"timeouts_enabled" : True
		"day_distribution_file" : "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/day_distribution_pickled.txt",
		"week_distribution_file" : "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/week_distribution_pickled.txt"
		}
	return invariant_dict













#def compare_n_params(invariant_dict, input_workloads, utilisations, scheduler_list, parameter_file_output_folder, param_file_base_name):


def params_final_utilisation_study():
	inv_dict = get_invariant_dict_utilisation_study()
	scheduler_list = ["shortest_remaining_first",
					  "longest_remaining_first",
					  "random",
					  "fifo_task",
					  "fifo_job",
					  "fair_share",
					  "projected_slr",
					  "pslr_sf",
					  "pvd_sq_up",
					  "pvd_sq_down",
					  "projected_value_straight_up",
					  "projected_value_straight_down",
					  "projected_value_density_up",
					  "projected_value_density_down",
					  "texec_sum_value_density_up",
					  "texec_sum_value_density_down",
					  "projected_value_remaining_up",
					  "projected_value_remaining_down"]
	
	input_folder_base = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/workloads_june_2013/work2"
	
	#input_folder_base = "/Users/andy/Documents/Work/workloads_june_2013/work2"
	
	#eventual_input_folder_base = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/workloads_march_12/synthetics"
	
	filename_list = []
	
	for path, foldernames, filenames in os.walk(input_folder_base):
		for name in filenames:
			if name[0] != ".":
				filename_list.append(path + "/" + name)

	print len(filename_list), "workload files found"
	
	utilisations = [10, 20, 40, 60, 80, 90, 100, 110, 120]
	netcosts = [0.0] # [0.1, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
	
	estimate_errors = ["exact"]#, "normal_jitter", "log_round", "round"]
	error_values = [[100]]#, [10,20,50,150,200], [2,5,10,20], [1000,10000,100000]]
	
	output_folder = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/utilisation_sweep"
	arrival_patterns = ["static_pattern", "day_week_pattern"]
	timeouts_enabled = [True, False]
	
	#output_folder = "/Users/andy/Desktop/util_sweep"
	
	param_file_base_name = "params_"
	compare_n_params(inv_dict, filename_list, utilisations, netcosts, scheduler_list, output_folder, param_file_base_name,
					 estimate_errors, error_values, arrival_patterns, timeouts_enabled)



def params_final_netcosts_study():
	inv_dict = get_invariant_dict_utilisation_study()
	scheduler_list = ["shortest_remaining_first",
					  "longest_remaining_first",
					  "random",
					  "fifo_task",
					  "fifo_job",
					  "fair_share",
					  "projected_slr",
					  "pslr_sf",
					  "pvd_sq_up",
					  "pvd_sq_down",
					  "projected_value_straight_up",
					  "projected_value_straight_down",
					  "projected_value_density_up",
					  "projected_value_density_down",
					  "texec_sum_value_density_up",
					  "texec_sum_value_density_down",
					  "projected_value_remaining_up",
					  "projected_value_remaining_down"]
	
	input_folder_base = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/workloads_june_2013/work2"
	
	#input_folder_base = "/Users/andy/Documents/Work/workloads_june_2013/work2"
	
	#eventual_input_folder_base = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/workloads_march_12/synthetics"
	
	filename_list = []
	
	for path, foldernames, filenames in os.walk(input_folder_base):
		for name in filenames:
			if name[0] != ".":
				filename_list.append(path + "/" + name)
	
	print len(filename_list), "workload files found"
	
	utilisations = [90, 95, 100] #[10, 20, 40, 60, 80, 90, 100, 110, 120]
	netcosts = [0.1, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
	
	estimate_errors = ["exact"]#, "normal_jitter", "log_round", "round"]
	error_values = [[100]]#, [10,20,50,150,200], [2,5,10,20], [1000,10000,100000]]
	
	output_folder = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/lastnetparams"
	arrival_patterns = ["day_week_pattern"]
	timeouts_enabled = [True]
	
	#output_folder = "/Users/andy/Desktop/util_sweep"
	
	param_file_base_name = "params_"
	compare_n_params(inv_dict, filename_list, utilisations, netcosts, scheduler_list, output_folder, param_file_base_name,
					 estimate_errors, error_values, arrival_patterns, timeouts_enabled)



def params_final_inac_study():
	inv_dict = get_invariant_dict_utilisation_study()
	scheduler_list = ["shortest_remaining_first",
					  "longest_remaining_first",
					  "random",
					  "fifo_task",
					  "fifo_job",
					  "fair_share",
					  "projected_slr",
					  "pslr_sf",
					  "pvd_sq_up",
					  #"pvd_sq_down",
					  #"projected_value_straight_up",
					  "projected_value_straight_down",
					  "projected_value_density_up",
					  "projected_value_density_down",
					  "texec_sum_value_density_up",
					  "texec_sum_value_density_down",
					  #"projected_value_remaining_up",
					  "projected_value_remaining_down"]
	
	input_folder_base = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/w2"
	
	#input_folder_base = "/Users/andy/Documents/Work/workloads_june_2013/work2"
	
	#eventual_input_folder_base = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/workloads_march_12/synthetics"
	
	filename_list = []
	
	for path, foldernames, filenames in os.walk(input_folder_base):
		for name in filenames:
			if name[0] != ".":
				filename_list.append(path + "/" + name)
	
	print len(filename_list), "workload files found"
	
	utilisations = [90, 100, 110] #[10, 20, 40, 60, 80, 90, 100, 110, 120]
	netcosts = [0.01, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99]
	
	estimate_errors = ["exact"]#, "normal_jitter", "log_round"]#], "round"]
	error_values = [[100]]#, [10,20,50,150,200], [2,5,10,20]]#, [1000,10000,100000]]
	
	output_folder = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/netfinal_params"
	arrival_patterns = ["day_week_pattern"]#["static_pattern", "day_week_pattern"]
	timeouts_enabled = [False] #[False, True]
	
	#output_folder = "/Users/andy/Desktop/util_sweep"
	
	param_file_base_name = "params_"
	compare_n_params(inv_dict, filename_list, utilisations, netcosts, scheduler_list, output_folder, param_file_base_name,
					 estimate_errors, error_values, arrival_patterns, timeouts_enabled)








if __name__ == "__main__":
	print "generating parameter files"
	#params_final_utilisation_study()
	params_final_inac_study()
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