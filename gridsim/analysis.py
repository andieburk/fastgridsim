#post-simulation analysis
import collections
import matplotlib
import matplotlib.pyplot as plt
import numpy
import os
from scipy import stats
import cPickle as pickle
import copy
import random
from types import *

run_match_string = "*.txt"
run_log_folder = "/Volumes/Red 500 GB/sim_data/netf_anal/"
analysis_folder =  "/Volumes/Red 500 GB/sim_data/netf_anal/"
analysis_aggregate_file_name = "filtered_output_2.txt"
chart_output_path =  "/Users/andie/Documents/Versioned_Work/Thesis/post_viva_recharts/"
pickle_filename = "analysis_pickled"
#processing_dir = "/Users/andy/Documents/Work/journal_paper_results/main_runlogs/"

analysis_aggregate_file_path = analysis_folder + analysis_aggregate_file_name
pickle_filepath = analysis_folder + pickle_filename


def analysis_orig():
	import os

	run_match_string = "*.txt"
	run_log_folder = "/Users/andy/Documents/Work/conf_paper_anal/run_results/"
	analysis_folder = "/Users/andy/Documents/Work/conf_paper_anal/run_results_analysis/"
	analysis_aggregate_file_name = "filtered_output.txt"
	processing_dir = "/Users/andy/Documents/Work/conf_paper_anal/"


	os.system("grep \#-\# " + run_log_folder + run_match_string + " >  " + analysis_folder + analysis_aggregate_file_name)

	#read file to analyse
	res_file = open(processing_dir + "filtered_output.txt", 'r')
	results_lines = res_file.readlines()

	print len(results_lines)
	print results_lines[0:3]



def extract_newer():
	run_pattern = run_log_folder + run_match_string
	
	runstr = "grep \#Results_Output\# " + run_pattern + " > " + analysis_aggregate_file_path
	print runstr
	#os.system("ls -l " + run_pattern + " > " + run_log_folder + "filelist.txt")
	os.system(runstr)

def extract_even_newer():
	out_file = open(analysis_aggregate_file_path, 'w')
	
	files_to_analyse = [f for f in os.listdir(run_log_folder) if f[0] != '.']
	
	for f in files_to_analyse:
		fpath = run_log_folder + f
		res_file = open(fpath, 'r')
		for line in res_file:
			if line[0:16] == "#Results_Output#":
				out_file.write(fpath + ":" + line)
		res_file.close()
	
	out_file.close()



def box_of_mean_parameters(results_dicts, parameter, log=False, list_parameter = False):

	schedulers = set((d["cluster_orderer"] for d in results_dicts))
	utilisations = set((d["target_utilisation"] for d in results_dicts))
	
			
	colour_dict = {"projected_slr" : "b", "fifo_task" : "g", "fifo_job" : "r", "random" : "#000066", "fair_share" : "#732C7B", "identity" : "black",
		"shortest_remaining_first": "#7F3D17", "longest_remaining_first": "#F20056"}
	
	
	scheduler_params = collections.defaultdict(lambda : [])
	
	for d in results_dicts:
		scheduler_params[d["cluster_orderer"]].append(numpy.float32(d[parameter]))
	
	#for s in scheduler_cum_completions.items():
	#	print s[0], sum(s[1])
	
	sym_str = ""
	whiskers = 1.5
	
	if list_parameter:
		for slr_list in scheduler_params.items():
			sl1 = [numpy.array([numpy.float32(i) for i in l]) for l in slr_list[1]]

			all_slrs_in_one = numpy.concatenate(sl1)

			scheduler_params[slr_list[0]] = all_slrs_in_one
			
			sym_str = "+"
			whiskers = 1000000
	



	for s in scheduler_params.items():
		scheduler_params[s[0]] = numpy.array(scheduler_params[s[0]])
	#print scheduler_params.values()


	fig = plt.figure()
	ax = fig.add_subplot(111)
	if log:
		ax.set_yscale('log')
	plt.boxplot(scheduler_params.values(), sym=sym_str, whis=whiskers)
	base_offset = [x+1 for x in range(len(scheduler_params.keys()))]
	plt.xticks(base_offset, scheduler_params.keys(), rotation='vertical')
	#plt.title(parameter)

	
	plt.show()
	
	fig.savefig(chart_output_path + parameter + ".pdf", format='pdf')#, bbox_inches='tight')
			
	
			
	proj_slr_values = scheduler_params["projected_slr"]

	if len(proj_slr_values) > 1:
				
		print "stat sig test for ", parameter, "is list parameter", list_parameter
		for s in scheduler_params.items():
			if s[0] != "projected_slr":
				ttest_rel_val = stats.ttest_rel(proj_slr_values, s[1])
				wilcoxon_val = stats.wilcoxon(proj_slr_values, s[1])
				print "sig test for __ against proj_slr. repeated measures t-test then wilcoxon", s[0], ttest_rel_val, wilcoxon_val
	
			
			
	
	"""
	base = [x for x in range(5)]
	base_offset = [x+0.4 for x in range(5)]
	values = [sum(s[1]) / float(len(s[1])) for s in scheduler_params.items()]
	error_mins = [min(s[1]) for s in scheduler_params.items()]
	error_maxs = [max(s[1]) for s in scheduler_params.items()]
	
	
	plt.bar(base,values, log=True)
	plt.xticks(base_offset, scheduler_params.keys(), rotation='vertical')
	
	plt.errorbar(base_offset, values, yerr=[error_mins, error_maxs], fmt='bx')
	
	
	plt.show()




	boxplot(a)
	"""



def bar_of_mean_parameters(results_dicts, parameter, logscale=False):
	
	def autolabel(rects):
		# attach some text labels
		for rect in rects:
			height = rect.get_height()
			plt.text(rect.get_x()+rect.get_width()/2., 1.1*height, '%.1f'%height,
					ha='center', va='bottom')


	schedulers = set((d["cluster_orderer"] for d in results_dicts))
	utilisations = set((d["target_utilisation"] for d in results_dicts))
	
			
	colour_dict = {"projected_slr" : "b", "fifo_task" : "g", "fifo_job" : "r", "random" : "yellow", "fair_share" : "purple", "identity" : "black",
					"shortest_remaining_first": "peach", "longest_remaining_first": "fucsia"}
	
	
	
	scheduler_params = collections.defaultdict(lambda : [])
	
	for d in results_dicts:
		scheduler_params[d["cluster_orderer"]].append(d[parameter])
	
	#for s in scheduler_cum_completions.items():
	#	print s[0], sum(s[1])
	
	base = [x for x in range(7)]
	base_offset = [x+0.4 for x in range(7)]
	values = [sum(s[1]) / float(len(s[1])) for s in scheduler_params.items()]
	error_mins = [min(s[1]) for s in scheduler_params.items()]
	error_maxs = [max(s[1]) for s in scheduler_params.items()]


	print values
	
	fig = plt.figure()
	rects = plt.bar(base,values, log=logscale)
	plt.xticks(base_offset, scheduler_params.keys(), rotation='vertical')

	autolabel(rects)
	#plt.errorbar(base_offset, values, yerr=[error_mins, error_maxs], fmt='bx')
	

	plt.show()
	fig.savefig(chart_output_path + "bar_of_mean"+parameter+".pdf", format='pdf')


	#fig.savefig(chart_output_path + "bar_of_mean"+parameter+".pdf", format='pdf')



def decile_mean_slr_fig(results_dicts, utilisations, key = None):
	
	interested_utils = utilisations
	
	scheduler_quartile_slr_means = collections.defaultdict(lambda : [])
	scheduler_quartile_slr_stddevs = collections.defaultdict(lambda : [])
	
	print len(results_dicts)
	
	for d in results_dicts:
		if d["target_utilisation"] in interested_utils:
			scheduler_quartile_slr_means[d["cluster_orderer"]].append(d["decile_slr_means"])
			scheduler_quartile_slr_stddevs[d["cluster_orderer"]].append(d["decile_slr_stddevs"])
			print d["cluster_orderer"]
		else:
			print 'not', d["cluster_orderer"]
		
	
	#print scheduler_quartile_slr_means.items()
	
	sqsm_means = []
	sqsd_means = []
	
	
	print len(scheduler_quartile_slr_means)
	for sched_line in scheduler_quartile_slr_means.items():
		
		this_tuple = (sched_line[0], [])
		
		for ind in range(10):
			average_decile = numpy.average([s[ind] for s in sched_line[1]])
			this_tuple[1].append(average_decile)
		
		sqsm_means.append(this_tuple)
	
	
	colour_dict = {"projected_slr" : "b", "fifo_task" : "g", "fifo_job" : "r", "random" : "#000066", "fair_share" : "#732C7B", "identity" : "black",
		"shortest_remaining_first": "#7F3D17", "longest_remaining_first": "#F20056"}

	
	
	print sqsm_means

	fig = plt.figure()
	ax = fig.add_subplot(111)

	
	markers = ['o', '+', 'x', '*', '^', '<', '>']
	linestyles = [':', ':', ':', ':', ':', ':', ':']
	
	id = 0

	print len(sqsm_means)
	
	
	for s in sqsm_means:
		plt.semilogy(range(1,11),(s[1]), label=s[0], marker=markers[id],  color=colour_dict[s[0]]) #linestyle=linestyles[id],
		id+=1
	
	plt.legend()
	#plt.title("Mean SLR by decile of job execution time for "+str(utilisations[0]) +"% utilisation" )
	plt.xlabel("decile of job execution time")
	plt.ylabel("mean slr for decile", rotation="vertical")

	
	#plt.show()
	
	
	
	#plt.savefig(chart_output_path + "decile_mean_slrs")

	
	

	#if log:
	#	ax.set_yscale('log')
	#plt.boxplot(scheduler_params.values(), sym="")
	#base_offset = [x+1 for x in range(5)]
	#plt.xticks(base_offset, scheduler_params.keys(), rotation='vertical')
	#plt.title(parameter)
	#plt.savefig("
	
	plt.show()
	
	#fig.savefig(chart_output_path + parameter + ".pdf", format='pdf')#, bbox_inches='tight')

	fig.savefig(chart_output_path + "decile_mean_slrs_120.pdf", format='pdf')


def decile_worst_slr_fig(results_dicts, utilisations):
	
	interested_utils = utilisations
	
	scheduler_quartile_slr_means = collections.defaultdict(lambda : [])
	#scheduler_quartile_slr_stddevs = collections.defaultdict(lambda : [])
	
	
	for d in results_dicts:
		if d["target_utilisation"] in interested_utils:
			scheduler_quartile_slr_means[d["cluster_orderer"]].append(d["decile worst slr"])
			#scheduler_quartile_slr_stddevs[d["cluster_orderer"]].append(d["decile_slr_stddevs"])
	
	
	#print scheduler_quartile_slr_means.items()
	
	sqsm_means = []
	sqsd_means = []
	
	for sched_line in scheduler_quartile_slr_means.items():
		
		this_tuple = (sched_line[0], [])
		
		for ind in range(10):
			average_decile = numpy.average([s[ind] for s in sched_line[1]])
			this_tuple[1].append(average_decile)
		
		sqsm_means.append(this_tuple)
	
	
	
	print sqsm_means
	
	fig = plt.figure()
	ax = fig.add_subplot(111)
	
	
	markers = ['o', '+', 'x', '*', '^', '>', '<']
	linestyles = [':', ':', ':', ':', ':', ':', ':']
	
	id = 0
	
		
	colour_dict = {"projected_slr" : "b", "fifo_task" : "g", "fifo_job" : "r", "random" : "#000066", "fair_share" : "#732C7B", "identity" : "black",
		"shortest_remaining_first": "#7F3D17", "longest_remaining_first": "#F20056"}

	
	
	for s in sqsm_means:
		plt.semilogy(range(1,11),(s[1]), label=s[0],  color=colour_dict[s[0]]) #marker=markers[id], linestyle=linestyles[id],
		id+=1
	
	plt.legend()
	#plt.title("Worst-case SLR by decile of job execution time for "+str(utilisations[0]) +"% utilisation" )
	plt.xlabel("decile of job execution time")
	plt.ylabel("worst-case slr for decile", rotation="vertical")
	
	
	#plt.show()
	
	
	
	#plt.savefig(chart_output_path + "decile_mean_slrs")
	
	
	
	
	#if log:
	#	ax.set_yscale('log')
	#plt.boxplot(scheduler_params.values(), sym="")
	#base_offset = [x+1 for x in range(5)]
	#plt.xticks(base_offset, scheduler_params.keys(), rotation='vertical')
	#plt.title(parameter)
	#plt.savefig("
	
	plt.show()
	
	#fig.savefig(chart_output_path + parameter + ".pdf", format='pdf')#, bbox_inches='tight')
	
	fig.savefig(chart_output_path + "decile_worst_slrs_120.pdf", format='pdf')


	
	
def mean_slr_over_utilisation(results_dicts, workload_kinds):
	
	
	mean_slr_dict = collections.defaultdict(lambda : [])
	
		
	colour_dict = {"projected_slr" : "b", "fifo_task" : "g", "fifo_job" : "r", "random" : "yellow", "fair_share" : "purple", "identity" : "black",
					"shortest_remaining_first": "black", "longest_remaining_first":"black"}
	
	
	#for w in workload_kinds:
		
		
	#for res in all_results:
	#	if res[2] == w:
	#		mean_slr_dict[(res[0], res[1])].append(res[value_to_plot_index])


	for d in results_dicts:
		#if d["workload"] == w:
		mean_slr_dict[(d["target_utilisation"], d["cluster_orderer"])].append(d["mean_slr"])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d["median_slr"])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d[""])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d["mean_slr"])


	
#	print "mean_slr_dict"
#	print mean_slr_dict.items()[1:10]
	
#	exit()
	
	ress_to_plot = collections.defaultdict(lambda : [])
	print len(mean_slr_dict.items())
	
	for i in mean_slr_dict.items():
		print i
		mean_mean_slrs = numpy.average([numpy.float32(j) for j in i[1]])
		
		print i[0][1]
		print i[0][0]
		
		ress_to_plot[i[0][1]].append([int(i[0][0]), mean_mean_slrs])
	

	fig = plt.figure()
	
	print ress_to_plot.items()

	for i in ress_to_plot.items():
	
		sorted_i1 = sorted(i[1], key=lambda x: x[0])
	
		utils = [j[0] for j in sorted_i1]
		means = [j[1] for j in sorted_i1]
		plt.semilogy(utils, means, colour_dict[i[0]], label=i[0])
		plt.xticks(range(80,130, 10))
		plt.legend(loc=2)
		plt.xlabel("Stability Ratio")
		plt.ylabel("Mean SLR", rotation='vertical')
		#plt.title("Mean SLR changes with Stability Ratio")
		
	
	plt.show()

	fig.savefig(chart_output_path + "mean_slr_by_utilisation.pdf", format="pdf")



def mean_each_unit_slr_over_utilisation(results_dicts, workload_kinds):
	
	
	mean_slr_dict = collections.defaultdict(lambda : [])
	
		
	colour_dict = {"projected_slr" : "b", "fifo_task" : "g", "fifo_job" : "r", "random" : "yellow", "fair_share" : "purple", "identity" : "black",
					"shortest_remaining_first": "black", "longest_remaining_first":"black"}
	
	
	#for w in workload_kinds:
		
		
	#for res in all_results:
	#	if res[2] == w:
	#		mean_slr_dict[(res[0], res[1])].append(res[value_to_plot_index])


	for d in results_dicts:
		#if d["workload"] == w:
		mean_slr_dict[(d["target_utilisation"], d["cluster_orderer"])].append(d["z_all"])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d["median_slr"])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d[""])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d["mean_slr"])


	
#	print "mean_slr_dict"
#	print mean_slr_dict.items()[1:10]
	
#	exit()
	
	ress_to_plot = collections.defaultdict(lambda : [])
	print len(mean_slr_dict.items())
	
	for i in mean_slr_dict.items():
		print i
		mean_mean_slrs = numpy.average([numpy.float32(j) for j in i[1]])
		
		print i[0][1]
		print i[0][0]
		
		ress_to_plot[i[0][1]].append([int(i[0][0]), mean_mean_slrs])
	

	fig = plt.figure()
	
	print ress_to_plot.items()

	for i in ress_to_plot.items():
	
		sorted_i1 = sorted(i[1], key=lambda x: x[0])
	
		utils = [j[0] for j in sorted_i1]
		means = [j[1] for j in sorted_i1]
		plt.semilogy(utils, means, colour_dict[i[0]], label=i[0])
		plt.xticks(range(80,130, 10))
		plt.legend(loc=2)
		plt.xlabel("Stability Ratio")
		plt.ylabel("Mean SLR", rotation='vertical')
		#plt.title("Mean SLR changes with Stability Ratio")
		
	
	plt.show()

	fig.savefig(chart_output_path + "mean_unit_slr_by_utilisation.pdf", format="pdf")




def mean_worst_slr_over_utilisation(results_dicts, workload_kinds):
	
	
	mean_slr_dict = collections.defaultdict(lambda : [])
	
		
	colour_dict = {"projected_slr" : "b", "fifo_task" : "g", "fifo_job" : "r", "random" : "#000066", "fair_share" : "#732C7B", "identity" : "black",
		"shortest_remaining_first": "#7F3D17", "longest_remaining_first": "#F20056"}
	
	#for w in workload_kinds:
		
		
	#for res in all_results:
	#	if res[2] == w:
	#		mean_slr_dict[(res[0], res[1])].append(res[value_to_plot_index])


	for d in results_dicts:
		#if d["workload"] == w:
		mean_slr_dict[(d["target_utilisation"], d["cluster_orderer"])].append(d["maximum_slr"])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d["median_slr"])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d[""])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d["mean_slr"])


	
#	print "mean_slr_dict"
#	print mean_slr_dict.items()[1:10]
	
#	exit()
	
	ress_to_plot = collections.defaultdict(lambda : [])
	print len(mean_slr_dict.items())
	
	for i in mean_slr_dict.items():
		print i
		mean_mean_slrs = numpy.average([numpy.float32(j) for j in i[1]])
		
		print i[0][1]
		print i[0][0]
		
		ress_to_plot[i[0][1]].append([int(i[0][0]), mean_mean_slrs])
	

	fig = plt.figure()
	
	#print ress_to_plot.items()
	
	markers = ['o', '+', 'x', '*', '^', '>', '<']
	
	j = -1
	for i in ress_to_plot.items():
		j += 1
		sorted_i1 = sorted(i[1], key=lambda x: x[0])
	
		utils = [j[0] for j in sorted_i1]
		means = [j[1] for j in sorted_i1]
		plt.semilogy(utils, means, colour_dict[i[0]], label=i[0])
		plt.xticks(range(80,130, 10))
		plt.legend(loc=2)
		plt.xlabel("Stability Ratio")
		plt.ylabel("Mean Worst-Case SLR", rotation='vertical')
		#plt.title("Worst-case SLR changes with Stability Ratio")
		
	
	plt.show()

	fig.savefig(chart_output_path + "mean_worstcase_slr_by_utilisation.pdf", format="pdf")	
	


def worst_worst_slr_over_utilisation(results_dicts, workload_kinds):
	
	
	mean_slr_dict = collections.defaultdict(lambda : [])
	
		
	colour_dict = {"projected_slr" : "b", "fifo_task" : "g", "fifo_job" : "r", "random" : "#000066", "fair_share" : "#732C7B", "identity" : "black",
		"shortest_remaining_first": "#7F3D17", "longest_remaining_first": "#F20056"}
	
	
	#for w in workload_kinds:
		
		
	#for res in all_results:
	#	if res[2] == w:
	#		mean_slr_dict[(res[0], res[1])].append(res[value_to_plot_index])


	for d in results_dicts:
		#if d["workload"] == w:
		mean_slr_dict[(d["target_utilisation"], d["cluster_orderer"])].append(d["maximum_slr"])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d["median_slr"])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d[""])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d["mean_slr"])


	
#	print "mean_slr_dict"
#	print mean_slr_dict.items()[1:10]
	
#	exit()
	
	ress_to_plot = collections.defaultdict(lambda : [])
	print len(mean_slr_dict.items())
	
	for i in mean_slr_dict.items():
		print i
		mean_mean_slrs = numpy.amax([numpy.float32(j) for j in i[1]])
		
		print i[0][1]
		print i[0][0]
		
		ress_to_plot[i[0][1]].append([int(i[0][0]), mean_mean_slrs])
	

	fig = plt.figure()
	
	print ress_to_plot.items()

	for i in ress_to_plot.items():
	
		sorted_i1 = sorted(i[1], key=lambda x: x[0])
	
		utils = [j[0] for j in sorted_i1]
		means = [j[1] for j in sorted_i1]
		plt.semilogy(utils, means, colour_dict[i[0]], label=i[0])
		plt.xticks(range(80,130, 10))
		plt.legend(loc=2)
		plt.xlabel("Stability Ratio")
		plt.ylabel("Worst-case SLR", rotation='vertical')
		#plt.title("Worst-case SLR changes with Stability Ratio")
		
	
	plt.show()

	fig.savefig(chart_output_path + "worst_worst_slr_by_utilisation.pdf", format="pdf")




def worst_worst_slr_over_net_costs(results_dicts):
	
	
	mean_slr_dict = collections.defaultdict(lambda : [])
	
		
	colour_dict = {"projected_slr" : "b", "fifo_task" : "g", "fifo_job" : "r", "random" : "#000066", "fair_share" : "#732C7B", "identity" : "black",
		"shortest_remaining_first": "#7F3D17", "longest_remaining_first": "#F20056"}
	
	
	#for w in workload_kinds:
		
		
	#for res in all_results:
	#	if res[2] == w:
	#		mean_slr_dict[(res[0], res[1])].append(res[value_to_plot_index])


	for d in results_dicts:
		#if d["workload"] == w:
		mean_slr_dict[(d["communication_to_computation_ratio"], d["cluster_orderer"])].append(d["maximum_slr"])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d["median_slr"])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d[""])
	#mean_slr_dict[(d["scheduler"], d["target_utilisation"])].append(d["mean_slr"])


	
#	print "mean_slr_dict"
	print mean_slr_dict.items()[1:10]
	
	#exit()
	
	ress_to_plot = collections.defaultdict(lambda : [])
	print len(mean_slr_dict.items())
	
	for i in mean_slr_dict.items():
		print i
		mean_mean_slrs = numpy.amax([numpy.float32(j) for j in i[1]])
		
		print i[0][1]
		print i[0][0]
		
		ress_to_plot[i[0][1]].append([int(i[0][0]), mean_mean_slrs])
	

	fig = plt.figure()
	
	print ress_to_plot.items()

	for i in ress_to_plot.items():
	
		sorted_i1 = sorted(i[1], key=lambda x: x[0])
	
		utils = [j[0] for j in sorted_i1]
		means = [j[1] for j in sorted_i1]
		plt.semilogy(utils, means, colour_dict[i[0]], label=i[0])
		#plt.xticks(range(80,130, 10))
		plt.legend(loc=2)
		plt.xlabel("ccr")
		plt.ylabel("Worst-case SLR", rotation='vertical')
		#plt.title("Worst-case SLR changes with Stability Ratio")
		
	
	plt.show()

	fig.savefig(chart_output_path + "worst_worst_slr_by_utilisation.pdf", format="pdf")





def analysis_each_schedule():
	res_file_path = analysis_aggregate_file_path
	res_file = open(analysis_aggregate_file_path, 'r')

	res_all_slrs = collections.defaultdict(lambda : [])	
	res_proj_slrs = collections.defaultdict(lambda : [])
	res_all_slrs_by_scheduler = collections.defaultdict(lambda : [])
	
	results_dicts = []
	print "reading files in"
	for line in res_file:
		entries = line.split('|')

		#types = set([e.split(':')[2] for e in entries])
		

		
		new_res_dict = {}
		for e in entries:
			#print "doing an entry"
		
			e_parts = e.split(":")
			
			value = e_parts[1]
			
			incl = True
			
			if e_parts[2] == "int":
				value = int(value)
			elif e_parts[2] == "numpy.float64":
				value = float(value)
			elif e_parts[2] == "float":
				value = float(value)
			elif e_parts[2] == "long":
				value = long(value)
			elif e_parts[2] == "list":
				valuelist = eval(value)
				
				value_arr = numpy.array([numpy.float32(n) for n in valuelist])
				
				value = value_arr
				#if len(value) > 20:
				#	incl = False
			if incl:
				new_res_dict[e_parts[0]] = value
		
			
		new_arr = numpy.array([numpy.float32(slr_val) for slr_val in new_res_dict["z_all job slrs"]])
		
		res_all_slrs_by_scheduler[new_res_dict["cluster_orderer"]].append(new_arr)
							
		"""	
		if new_res_dict["cluster_orderer"] == "projected_slr":
			res_proj_slrs[new_res_dict["target_utilisation"]].append((new_res_dict["workload_filename"], new_res_dict["z_all job slrs"]))
			
			#print res_proj_slrs
			#exit()
			
		else:
			res_all_slrs[(new_res_dict["target_utilisation"], new_res_dict["cluster_orderer"])].append((new_res_dict["workload_filename"], new_res_dict["z_all job slrs"]))
			#print res_all_slrs
			#exit()
		"""
	

	
	
	
	
	
	
	
			
			
	#trying to calculate
	
	"""
	for each scheduler that isnt proj-slr, for each utilisation.
	
	
	all slrs arrays - sorted by workload name - all concatenated.
	
	calclate % of slrs for which proj SLR was better for each job
	
		--separate
	calulate % of slrs for which proj slr worst case was best
					
	"""	
	
	utilisations = [120]
	other_schedulers = ["shortest_remaining_first"]
	
	percent_that_slr_was_better = {}
	
	
	for u in utilisations:
		workloads_and_timings = res_proj_slrs[u]
		sorted_wts = sorted(workloads_and_timings, key=lambda x: x[0])
		all_ordered_slrs = (a[1] for a in sorted_wts)
		
		
		all_ordered_slrs_joined = []
		for b in all_ordered_slrs:
			all_ordered_slrs_joined.extend(b)
		
		pslr_ordered_slrs_array = numpy.array(all_ordered_slrs_joined)
		
		
		for s in other_schedulers:
			print s
			workloads_and_timings = res_all_slrs[(u, s)]
			sorted_wts = sorted(workloads_and_timings, key=lambda x: x[0])
			all_ordered_slrs = (a[1] for a in sorted_wts)
			
			all_ordered_slrs_joined = []
			for b in all_ordered_slrs:
				all_ordered_slrs_joined.extend(b)
			
			all_ordered_slrs_array = numpy.array(all_ordered_slrs_joined)
			
			dominated_array = pslr_ordered_slrs_array <= all_ordered_slrs_array
			
			percent_dominated_by_pslr = numpy.count_nonzero(dominated_array) * 100.0 / len(dominated_array)
			
			differences_array = all_ordered_slrs_array - pslr_ordered_slrs_array
			diffs_2 = pslr_ordered_slrs_array - all_ordered_slrs_array
			
			print pslr_ordered_slrs_array[0:40]
			print all_ordered_slrs_array[0:40]
			
			print dominated_array[0:40]
			
			print numpy.count_nonzero(dominated_array)
			print percent_dominated_by_pslr
			
			print stats.ttest_ind(pslr_ordered_slrs_array,all_ordered_slrs_array)
			print stats.wilcoxon(differences_array)
			print stats.wilcoxon(diffs_2)
			print stats.mannwhitneyu(pslr_ordered_slrs_array, all_ordered_slrs_array)
			print stats.wilcoxon(pslr_ordered_slrs_array, all_ordered_slrs_array)
			print stats.normaltest(pslr_ordered_slrs_array)
			
			print stats.normaltest(all_ordered_slrs_array)
			print stats.normaltest(differences_array)
			
			
			#pslr
			
				
				
		
		
		
		
			
		#results_dicts.append(new_res_dict)



def worst_case_slr_stat_sig_diff(results_dicts, key = None):
	
	if key == None:	
		key = "maximum_slr"
	
	
	max_slr_dict = collections.defaultdict(lambda : [])
	other_slr_max_dict = {}
	proj_slr_max_dict = {}
	for d in results_dicts:
		#if d["workload"] == w:
		max_slr_dict[(d["target_utilisation"], d["cluster_orderer"])].append((d["workload_filename"], d[key]))	


	#utilisations = [80, 90, 100, 110, 120]
	
	proj_slr_max_by_util_and_workload = {}
	
	others = collections.defaultdict(lambda : [])
	
	for m in max_slr_dict.items():
	
		maxes_sorted_by_workload = sorted(m[1], key=lambda x: x[0])
		maxes_array = numpy.array([numpy.float32(mx[1]) for mx in maxes_sorted_by_workload])
	
		if m[0][1] == "projected_slr":
			proj_slr_max_by_util_and_workload[m[0][0]] = maxes_array
		else:
			others[m[0][0]].append((m[0][1], maxes_array))
	
				
				
	res_dict1 = {}
				
	for u in others.keys():
		
		
		
		
		#print u
		#print others[u]
		for s in others[u]:
			utilisation = u
			scheduler = s[0]
			values = s[1]
			
			proj_slr_values = proj_slr_max_by_util_and_workload[u]
			
			print utilisation
			print scheduler
			#print values
			#print proj_slr_values
			
			dominated_array = proj_slr_values <= values
			percent_dominated_by_pslr = numpy.count_nonzero(dominated_array) * 100.0 / len(dominated_array)
			
			print percent_dominated_by_pslr
			
			ttest_ind_val = stats.ttest_ind(values, proj_slr_values)
			ttest_rel_val = stats.ttest_rel(values, proj_slr_values)
			
			print "ttest_ind", ttest_ind_val
			print "ttest_rel", ttest_rel_val


			res_dict1[(utilisation, scheduler)] = (percent_dominated_by_pslr, ttest_rel_val, ttest_ind_val)
	
	for i in sorted(res_dict1.items(), key = lambda x: x[0][1]):
		print i



def all_sorted_slrs_by_scheduler(results_dicts):

	scheds = (r["cluster_orderer"] for r in results_dicts)

	schedulers = set(scheds)
	
	slrs_by_scheduler = collections.defaultdict(lambda: [])

	for r in results_dicts:
		slrs_by_scheduler[r["cluster_orderer"]].extend(r["z_all job slrs"])
	
	for s in slrs_by_scheduler:
		slrs_by_scheduler[s] = numpy.array([float(x) for x in slrs_by_scheduler[s]])
	
	
	#for s in slrs_by_scheduler.values():
	#	s.sort()
	
	
	print sum(slrs_by_scheduler["projected_slr"])
	
	
	counts_greater_slr = collections.defaultdict(lambda : [])

	for i in range(1,110):
		for s in schedulers:
			count_all = len(slrs_by_scheduler[s])
			count_filtered = (slrs_by_scheduler[s] <= float(i/10.0)).sum()
			
			#count_filtered = len([slr for slr in slrs_by_scheduler[s] if float(slr) <= float(i)])
			counts_greater_slr[s].append(100*(float(count_filtered) / float(count_all)))


#	for res in sorted(counts_greater_slr.items()):
#		print res




#	cgslr = collections.defaultdict(lambda: [0] * 11)
#	for x in counts_greater_slr.items():
#		cgslr[x[0][0]][x[0][1]] = x[1]
#
#	print cgslr


	fig = plt.figure()
	ax = fig.add_subplot(111)
	
	
	#markers = ['o', '+', 'x', '*', '^', '>', '<']
	#linestyles = [':', ':', ':', ':', ':', ':', ':']
	
	#id = 0





		
	colour_dict = {"projected_slr" : "b", "fifo_task" : "g", "fifo_job" : "r", "random" : "#000066", "fair_share" : "#732C7B", "identity" : "black",
		"shortest_remaining_first": "#7F3D17", "longest_remaining_first": "#F20056"}
	
	
	for s in schedulers:
		
	#	plt.loglog(sorted(slrs_by_scheduler[s], reverse=True), label=s,  color=colour_dict[s]) #marker=markers[id], linestyle=linestyles[id],
		#id+=1
		plt.plot(sorted([1.0/x for x in slrs_by_scheduler[s]], reverse=False)[:40000], label=s,  color=colour_dict[s])
		#plt.plot(counts_greater_slr[s], label=s, color=colour_dict[s])

		#plt.semilogy(slrs_by_scheduler[s])

	plt.legend()
	#plt.title("Worst-case SLR by decile of job execution time for "+str(utilisations[0]) +"% utilisation" )
	plt.xlabel("task")
	plt.ylabel("1/SLR", rotation="vertical")


	plt.show()


def mean_mean_slr_from_res(results_dicts):
	means = numpy.array([r["mean_slr"] for r in results_dicts])

	return numpy.mean(means)

def mean_wc_slr_from_res(results_dicts):
	means = numpy.array([r["worst case slr"] for r in results_dicts])

	return numpy.mean(means)






def net_disp(results_dicts):
	
	by = collections.defaultdict(lambda : collections.defaultdict(lambda: []))

	#partition by scheduler and ccr
	
	for r in results_dicts:
		by[r["cluster_orderer"]][r["communication_to_computation_ratio"]].append(r)
	
	print len(by)
	print by.keys()
	
	print len(by[by.keys()[0]])
	print by[by.keys()[0]].keys()
	
	







def inac_disp(results_dicts):

	print "inac disp"
	
	print set([r["communication_to_computation_ratio"] for r in results_dicts])
	
	
	
	
	#results_dicts = [r for r in results_dicts if r["communication_to_computation_ratio"] == 0.2]
	
	
	
	colour_dict = {"projected_slr" : "b", "fifo_task" : "g", "fifo_job" : "r", "random" : "#000066", "fair_share" : "#732C7B", "identity" : "black",
		"shortest_remaining_first": "#7F3D17", "longest_remaining_first": "#F20056"}

	normal_jitters = collections.defaultdict(lambda : [])
	log_rounds = collections.defaultdict(lambda : [])
	exacts = collections.defaultdict(lambda: [])
	others = collections.defaultdict(lambda: [])
	


	values = [1,1, 10, 20, 40, 60, 80, 100, 200, 1000, 10000, 100000, 1000000, 10000000, 100000000]

	new_dicts = []

	for r in results_dicts:	
		
		if r["exec_time_estimate_kind"] == 'exact':
			r["exec_time_estimate_kind"] = 'normal_jitter'
			r["exec_time_estimate_value"] = 1

			if r["cluster_orderer"] not in set(["projected_slr", "shortest_remaining_first", "longest_remaining_first"]):
				
				for v in values:
					
					new_r = copy.copy(r)
					new_r["exec_time_estimate_value"] = v
					new_dicts.append(new_r)


	results_dicts.extend(new_dicts)


				







	
	for r in results_dicts:	
	
		if r["exec_time_estimate_kind"] == 'normal_jitter':
			normal_jitters[(r["cluster_orderer"],r["exec_time_estimate_value"])].append(r)
		elif r["exec_time_estimate_kind"] == 'log_round':
			log_rounds[r["exec_time_estimate_value"]].append(r)
		elif r["exec_time_estimate_kind"] == 'exact':
			exacts[r["cluster_orderer"]].append(r)
		else:
			others[r["cluster_orderer"]].append(r)


	for s in exacts.keys():
		print s
		print mean_mean_slr_from_res(exacts[s])


	



	normal_means = collections.defaultdict(lambda : [])
	
	for val in sorted(normal_jitters.items(), key=lambda x : x[0][1]):
		normal_means[val[0][0]].append((val[0][1],mean_wc_slr_from_res(val[1])))

	for val in normal_means.items():
		
		a = zip(*val[1])
		
		print a
	
		plt.loglog(a[0],a[1], label=val[0], color = colour_dict[val[0]], marker='x')


	plt.legend()


	plt.show()

	
def line_plot(results_dicts, x_parameter, y_parameter, scheduler_parameter, include_filter_dict, exclude_filter_dict, applied_function, stat_test_function, plotter, yaxislimit=None, xaxislimit=None, extras = False, ylabel = None):
	
	
	
	
	if len(include_filter_dict) > 0:
		rd_positive = []
		for r in results_dicts:
			append = False
			for filter in include_filter_dict.items():
				if not append:
					filter_category = filter[0]
					filter_value = filter[1]
					if r[filter_category] == filter_value:
						append = True
			if append:
				rd_positive.append(r)
	else:
		rd_positive = results_dicts


	if len(exclude_filter_dict) > 0:
		rd_negative = []

		for r in rd_positive:
			append = True
			for filter in exclude_filter_dict.items():
				filter_category = filter[0]
				filter_value	= filter[1]
				if r[filter_category] == filter_value:
					append = False
			if append:
				rd_negative.append(r)
	else:
		rd_negative = rd_positive

	if "exec_time_estimate_kind" in exclude_filter_dict.keys():
		b = exclude_filter_dict["exec_time_estimate_kind"]
	else:
		b = ''
	
	if b == 'normal_jitter':
		a = 'log_round'
	elif b == 'log_round':
		a = 'normal_jitter'
	else:
		a = ''

	if extras:

		print set([r["exec_time_estimate_kind"] for r in results_dicts])
		#a = 'normal_jitter'
		
		if a == 'log_round':
			values = [2,3,4,5,10,20,100,1000,10000,100000,1000000,10000000]
		elif a == 'normal_jitter':
			values = [1,1, 10, 20, 40, 60, 80, 100, 200, 1000, 10000, 100000, 1000000, 10000000, 100000000]
		else:
			values = []

		new_dicts = []

		for r in results_dicts:	
			
			if r["exec_time_estimate_kind"] == 'exact':
			
				r2 = copy.copy(r)
			
				r2["exec_time_estimate_kind"] = a
				r2["exec_time_estimate_value"] = 1

				if r2["cluster_orderer"] not in set(["projected_slr", "shortest_remaining_first", "longest_remaining_first"]):
					
					for v in values:
						
						new_r = copy.copy(r2)
						new_r["exec_time_estimate_value"] = v
						new_dicts.append(new_r)

		print '############# new dicts', len(new_dicts)
		rd_negative.extend(new_dicts)


				


		

	rd_results = collections.defaultdict(lambda : collections.defaultdict(lambda: []))

	#filter by scheduler and group interesting values

	for r in rd_negative:
		rd_results[r[scheduler_parameter]][r[x_parameter]].append(r)


	results_to_plot = collections.defaultdict(lambda : [])


	results_to_stat_sig_test = collections.defaultdict(lambda : collections.defaultdict(lambda: []))

	for r in rd_results.items():
		for v in r[1].items():
			#print v[0]
			all_x_values = [dic[y_parameter] for dic in v[1]]
			
			if type(all_x_values[0]) == type('str'):
				all_x_values = [float(item_value) for item_value in all_x_values]
			
			final_value = applied_function(all_x_values)

			results_to_plot[r[0]].append((v[0], final_value))

			results_to_stat_sig_test[v[0]][r[0]] = all_x_values
			



	#print results_to_stat_sig_test.keys()
	#print [r.keys() for r in results_to_stat_sig_test.values()]
	

	test_table_results = {}
	
	#stat_sig_sched = "projected_slr"
	stat_sig_sched = "projected_value_remaining_down"
	
	
	
	for r in results_to_stat_sig_test.items():
		p_slr_xs = r[1][stat_sig_sched]
		others = [b for b in r[1].items() if b[0] != stat_sig_sched]
	
		#print 'others', others
		for o in others:
			
			other_array = o[1]
			
			lenp = len(p_slr_xs)
			leno = len(other_array)
			
			shorter_length = min([lenp, leno])
			
			if stat_test_function != None:
				p_slr_xs = stat_test_function(p_slr_xs)
				other_array = stat_test_function(other_array)
			
			p_slrs_sorted = sorted(p_slr_xs, reverse=True)[:shorter_length]
			other_array_sorted = sorted(other_array, reverse=True)[:shorter_length]
			
			
		
			#print len(p_slr_xs)
			#print len(o[1])
			
			#print r[0], o[0], stats.ttest_rel(p_slrs_sorted, other_array_sorted), stats.ttest_ind(p_slrs_sorted, other_array_sorted), stats.wilcoxon(p_slrs_sorted, other_array_sorted)
			
			ttest_rel_val = stats.ttest_rel(p_slrs_sorted, other_array_sorted)
			
			outval = (ttest_rel_val[1] < 0.05, ttest_rel_val[1])
			
			
			test_table_results[(r[0], o[0])] = outval

	print "stat sig diff for " + stat_sig_sched

	for t in test_table_results.items():
		print t




	print sorted(set([t[0] for t in test_table_results.keys()]))

	for y_par in sorted(set([t[0][1] for t in test_table_results.items()])):
	
		indexed_x_pairs = [(t[0][0], t[1][0]) for t in test_table_results.items() if t[0][1] == y_par]
		
		print y_par, [i[1] for i in sorted(indexed_x_pairs, key=lambda x : x[0])]
	
		#for t in test_table_results.items():
			


	#exit()

	print results_to_plot

	results_to_plot_formatted_right = {}

	for r in results_to_plot.items():
		sorted_results = sorted(r[1], key = lambda x: x[0])
	
		results_to_plot_formatted_right[r[0]] = zip(*sorted_results)

	print results_to_plot_formatted_right





	colour_dict = {
		"fair_share": "#732C7B" ,
		"fifo_job": "r" ,
		"fifo_task": "g" ,
		"random": "#5B5F01" ,
		"longest_remaining_first": "#F20056" ,
		"projected_slr": "b" ,
		"pslr_sf": "#73D100" ,
		"pvd_sq_up": "#967800",
		"pvd_sq_down":"#967800",
		"shortest_remaining_first": "#7F3D17",
		"projected_value_straight_up": "#E80CF0" ,
		"projected_value_straight_down": "#E80CF0" ,
		"projected_value_density_up": "#0C7EF0" ,
		"projected_value_density_down": "#0C7EF0" ,
		"texec_sum_value_density_up": "#370C9B" ,
		"texec_sum_value_density_down": "#370C9B" ,
		"projected_value_remaining_up": "#00B39B" ,
		"projected_value_remaining_down": "#00B39B" }

	tick_dict = {
			"fair_share": "s" ,
			"fifo_job": "+" ,
			"fifo_task": "." ,
			"longest_remaining_first": ">" ,
			"projected_slr": "x" ,
			"pslr_sf": "3" ,
			"pvd_sq_up": "o",
			"pvd_sq_down":"o",
			"random": "*" ,
			"shortest_remaining_first": "<" ,
			"projected_value_straight_up": "1" ,
			"projected_value_straight_down": "1" ,
			"projected_value_density_up": "2" ,
			"projected_value_density_down": "2" ,
			"texec_sum_value_density_up": "3" ,
			"texec_sum_value_density_down": "3" ,
			"projected_value_remaining_up": "4" ,
			"projected_value_remaining_down": "4" }






	#colour_dict = {"projected_slr" : "b", "fifo_task" : "g", "fifo_job" : "r", "random" : "#000066", "fair_share" : "#732C7B", "identity" : "black",
	#	"shortest_remaining_first": "#7F3D17", "longest_remaining_first": "#F20056"}


	#tick_dict = {"projected_slr" : "x", "fifo_task" : ".", "fifo_job" : "+", "random" : "*", "fair_share" : "s", "identity" : "o",
	#	"shortest_remaining_first": "<", "longest_remaining_first": ">"}


	if a == 'log_round':
		text_replacement_dict = {"mean_slr" : "Mean SLR",
							 "worst case slr" : "Median of worst-case Job SLR Values",
							 "communication_to_computation_ratio" : "Communication to Computation Ratio",
							 "gini_coeff_of_slrs" : "Median Gini Coefficient of Job SLR Values",
							 "exec_time_estimate_value" : "Base for log rounding inaccurate estimator",
							 "proportion_of_max_value": "Proportion of maximum value",
							 "proportion_of_max_value_nopenalty" : "Proportion of maximum value (no penalties applied)",
							 "proportion_of_max_value_vmaxpenalty" : "Proportion of maximum value (with penalties)",
							 "worst_case_slr" : "Median of worst-case Job SLR Values"
									 
							}

	elif a == 'normal_jitter':
		text_replacement_dict = {"mean_slr" : "Mean SLR",
							 "worst case slr" : "Median of worst-case Job SLR Values",
							 "communication_to_computation_ratio" : "Communication to Computation Ratio",
							 "gini_coeff_of_slrs" : "Median Gini Coefficient of Job SLR Values",
							 "exec_time_estimate_value" : "Standard deviation for normal distribution inaccurate estimator",
							 "proportion_of_max_value": "Proportion of maximum value",
							 "proportion_of_max_value_nopenalty" : "Proportion of maximum value (no penalties applied)",
							 "proportion_of_max_value_vmaxpenalty" : "Proportion of maximum value (with penalties)",
							 "worst_case_slr" : "Median of worst-case Job SLR Values"
				
							}

	else:
		text_replacement_dict = {"mean_slr" : "Mean SLR",
							 "worst case slr" : "Median of worst-case Job SLR Values",
							 "communication_to_computation_ratio" : "Communication to Computation Ratio",
							 "gini_coeff_of_slrs" : "Median Gini Coefficient of Job SLR Values",
							 "exec_time_estimate_value" : "Base for log rounding inaccurate estimator",
							 "proportion_of_max_value": "Proportion of maximum value",
							 "target_utilisation" : "Load",
							 "proportion_of_max_value_nopenalty" : "Proportion of maximum value (no penalties applied)",
							 "proportion_of_max_value_vmaxpenalty" : "Proportion of maximum value (with penalties)",
							 "starved_proportion" : "Percentage of jobs incomplete by final deadline",
							 "worst_case_slr" : "Median of worst-case Job SLR Values"
							
							}

	

	fig = plt.figure()
	axes_handle = fig.add_subplot('111')
	scheduler_renames = {
								"fair_share": "FairShare" ,
								"fifo_job": "FIFO Job" ,
								"fifo_task": "FIFO Task" ,
								"random": "Random" ,
								"longest_remaining_first": "LRTF" ,
								"projected_slr": "P-SLR" ,
								"pslr_sf": "P-SLR-SF" ,
								"pvd_sq_up": "PVDSQ",
								"pvd_sq_down":"PVDSQ",
								"shortest_remaining_first": "SRTF",
								"projected_value_straight_up": "PV" ,
								"projected_value_straight_down": "PV" ,
								"projected_value_density_up": "PVCPD" ,
								"projected_value_density_down": "PVCPD" ,
								"texec_sum_value_density_up": "PVD" ,
								"texec_sum_value_density_down": "PVD" ,
								"projected_value_remaining_up": "PVR" ,
								"projected_value_remaining_down": "PVR" }
								
								


	for p in sorted(results_to_plot_formatted_right.items(), key=lambda x : x[0]):
		print "plotting", scheduler_renames[p[0]]
		plotter(p[1][0], p[1][1], label=scheduler_renames[p[0]],  color=colour_dict[p[0]],  marker=tick_dict[p[0]], markersize=9)


	reverse_y_axis = False


	if reverse_y_axis:
		plt.gca().invert_yaxis()


	#plt.plot(counts_greater_slr[s], label=s, color=colour_dict[s])

	#plt.semilogy(slrs_by_scheduler[s])

	#plt.legend(loc='upper left', ncol=2)#, bbox_to_anchor=(0.5, -0.1), ncol=2)
	#plt.title("Worst-case SLR by decile of job execution time for "+str(utilisations[0]) +"% utilisation" )


	if x_parameter in text_replacement_dict.keys():
		xtext = text_replacement_dict[x_parameter]
	else:
		xtext = x_parameter

	if ylabel != None:
		xtext = ylabel


	if y_parameter in text_replacement_dict.keys():
		ytext = text_replacement_dict[y_parameter]
	else:
		ytext = y_parameter




	#plt.xticks(fontsize=18)
	plt.xticks(fontsize=18) #[80,90,100,110,120],
	
	plt.yticks(fontsize=18)

	plt.xlabel(xtext, fontsize=16)
	plt.ylabel(ytext, rotation="vertical", fontsize=16)

	if yaxislimit != None:
		axes_handle.set_autoscaley_on(False)
		axes_handle.set_ylim(yaxislimit)

	if xaxislimit != None:
		axes_handle.set_autoscalex_on(False)
		axes_handle.set_xlim(xaxislimit)



	plt.show()

	a = random.uniform(1,100000)

	filename = x_parameter + '_' + y_parameter + '_' + str(a) + '.pdf'


	

	fig.savefig(chart_output_path + filename, format='pdf')


















def analysis_even_newer():


	short_rd_pickle_filename = "short_net_pickled.pik"

	use_pickled = True
	use_short = False
	
	longlists = False
	
	if use_pickled:
		if not use_short:
			print "loading full pickle"
			results_dicts = pickle.load( open( pickle_filepath, "rb" ) )
		else:
			print "loading short pickle"
			results_dicts = pickle.load(open(analysis_folder + short_rd_pickle_filename, "rb"))
	else:


		
		res_file_path = analysis_aggregate_file_path
		res_file = open(analysis_aggregate_file_path, 'r')
		
		results_dicts = []
		print "reading files in"
		for line in res_file:
			entries = line.split('|')

			#types = set([e.split(':')[2] for e in entries])
			
			new_res_dict = {}
			for e in entries:
				e_parts = e.split(":")
				
				value = e_parts[1]
				
				incl = True
				
				if e_parts[2] == "int":
					value = int(value)
				elif e_parts[2] == "numpy.float64":
					value = float(value)
				elif e_parts[2] == "float":
					value = float(value)
				elif e_parts[2] == "long":
					value = long(value)
				elif e_parts[2] == "list":
					value = eval(value)
					if not longlists:
						if len(value) > 20:
							incl = False
				if incl:
					new_res_dict[e_parts[0]] = value
			results_dicts.append(new_res_dict)
		
		pickle.dump( results_dicts, open( pickle_filepath, "wb" ) )
	
	#print "extracting workload names"
	
	
	
	
	print len(results_dicts), "pickled run dicts loaded"



	#bit of a hack to extract workload names
	workload_kinds_list = []
	
	"""
	for d in results_dicts:
		input_file_path = d["workload_filename"]
		input_workload_kind = '_'.join(input_file_path.split('/')[-1].split('_')[0:-1])
		workload_kinds_list.append(input_workload_kind)
		d["workload"] = input_workload_kind
	
	workload_kinds = set(workload_kinds_list)
	
	"""

	"""
	applied_function = numpy.mean
	stat_test_function = None
	plotter = plt.plot
	"""

	line_plot(results_dicts, "communication_to_computation_ratio", "worst case slr", "cluster_orderer", {"router_allocator" : 'random'}, {}, numpy.median, None, plt.semilogy)
	
	line_plot(results_dicts, "communication_to_computation_ratio", "gini_coeff_of_slrs", "cluster_orderer", {"router_allocator" : 'random'}, {}, numpy.median, None, plt.plot)
	

	
	
	#line_plot(results_dicts, "exec_time_estimate_value", "mean slr", "cluster_orderer", {},  {"exec_time_estimate_kind" : 'log_round'}, numpy.mean, None, plt.loglog, extras = True)






	### known good for paper
	#line_plot(results_dicts, "exec_time_estimate_value", "gini_coeff_of_slrs", "cluster_orderer", {},  {"exec_time_estimate_kind" : 'log_round'}, numpy.median, None, plt.semilogx, extras = True)
	
	#line_plot(results_dicts, "exec_time_estimate_value", "gini_coeff_of_slrs", "cluster_orderer", {},  {"exec_time_estimate_kind" : 'normal_jitter'}, numpy.median, None, plt.semilogx, extras = True)
	
	#line_plot(results_dicts, "exec_time_estimate_value", "worst case slr", "cluster_orderer", {}, {"exec_time_estimate_kind" : 'log_round'}, numpy.median, None, plt.loglog, extras = True)
	
	#line_plot(results_dicts, "exec_time_estimate_value", "worst case slr", "cluster_orderer", {}, {"exec_time_estimate_kind" : 'normal_jitter'}, numpy.median, None, plt.loglog, extras = True)
	
	
	
	
	#end known good
	


	print results_dicts[0]

	
	for r in sorted(results_dicts[0].keys()):
		print r

	"""

	netcosts_found = set([])
	for r in results_dicts:
		if r["communication_to_computation_ratio"] not in netcosts_found:
			netcosts_found.add(r["communication_to_computation_ratio"])
	print 'netcosts found', netcosts_found

	"""
	#exit()
	#inac_disp(results_dicts)
	
	
	#net_disp(results_dicts)
	
	
	#worst_worst_slr_over_net_costs(results_dicts) #[r for r in results_dicts if r["exec_time_estimate_kind"] == "exact"])
	
	
	
	#for r in results_dicts:
	#	print r["cluster_orderer"]
	
	#print set((r["exec_time_estimate_kind"] for r in results_dicts))
	
	"""
	short_rd = [r for r in results_dicts if r["exec_time_estimate_kind"] == 'exact']


	pickle.dump( short_rd, open( analysis_folder + short_rd_pickle_filename, "wb" ) )
	"""
	

	"""
	have to be in final analysis:
	
	worst_case_slr_stat_sig_diff(results_dicts)
	box_of_mean_parameters(results_dicts, "cumulative completion", log=False)
	
	
	"""
	
	#worst_case_slr_stat_sig_diff(results_dicts)
	#worst_case_slr_stat_sig_diff(results_dicts, key="mean_slr")
	
	#box_of_mean_parameters(results_dicts, "average actual utilisation", log=False)
	
	#exit()
	
	#box_of_mean_parameters(results_dicts, "cumulative completion", log=False)
	#box_of_mean_parameters(results_dicts, "flow", log=True)
	
	#if longlists:
	#	box_of_mean_parameters(results_dicts, "z_all job slrs", log=True, list_parameter=True)
	#	box_of_mean_parameters(results_dicts, "z_all job speedups", log=True, list_parameter=True)
	
	


	"""
	box_of_mean_parameters(results_dicts, "mean speedup", log=False)
	box_of_mean_parameters(results_dicts, "mean stretch", log=True)
	
	box_of_mean_parameters(results_dicts, "mean_slr", log=True)

	#box_of_mean_parameters(results_dicts, "median_slr", log=True)
	
	
	box_of_mean_parameters(results_dicts, "peak in flight", log=False)
	
	
	
	box_of_mean_parameters(results_dicts, "stddev_slr", log=True)
	
	
	
	box_of_mean_parameters(results_dicts, "workload makespan", log=False)
	
	box_of_mean_parameters(results_dicts, "maximum_slr", log=True)
	"""
	print len(results_dicts)


	


	#box_of_mean_parameters(results_dicts, "z_all job slrs", log=True)
	
	#bar_of_mean_parameters(results_dicts, "stddev_slr", logscale=True)
	#bar_of_mean_parameters(results_dicts, "peak in flight", logscale=False)


	decile_mean_slr_fig(results_dicts, [120])
	#decile_worst_slr_fig(results_dicts, [120])
	
	
	#mean_slr_over_utilisation(results_dicts, workload_kinds)
	
	
	#mean_worst_slr_over_utilisation(results_dicts, workload_kinds)
	#worst_worst_slr_over_utilisation(results_dicts, workload_kinds)
	
	#all_sorted_slrs_by_scheduler(results_dicts)
	
	exit()
	
	
	
	
	
	




"""


def analysis_newer():
	processing_dir = "/Users/andy/Documents/Work/journal_paper_results/main_runlogs/"
	#read file to analyse
	res_file = open(processing_dir + "filtered_run_output.txt", 'r')
	res_file2 = open(processing_dir + "filtered_slr_bits.txt", 'r')
	
	slr_from_file2 = []
	slr_standard_dev_from_file_2 = []
	for line in res_file2:
		ls = line.split(' ')
		u = numpy.float32(ls[-2][1:-1])
		sd = numpy.float32(ls[-1][:-2])
		
		slr_from_file2.append(u)
		slr_standard_dev_from_file_2.append(sd)
	
	results_lines = res_file.readlines()

	#print len(results_lines)
	#print results_lines[0:3]
	
	parsing_1 = [l.split(',') for l in results_lines]
	#print parsing_1[0]
	parsing_2 = [l[0].split('/') for l in parsing_1]
	#print parsing_2[0]
	parsing_3 = [j[-1].split('_') for j in parsing_2]
	#print parsing_3[0]
	
	#exit()
	
	schedulers_parsed = []
	utilisations_parsed = []
	workload_parsed = []
	
	for p in parsing_3:
		if p[4] == "identity":
			schedulers_parsed.append(p[4])
			utilisations_parsed.append(p[5])
			workload_parsed.append('_'.join(p[6:-1]))
		else:
			schedulers_parsed.append('_'.join(p[4:6]))
			utilisations_parsed.append(p[6])
			workload_parsed.append('_'.join(p[7:-1]))
	

	print schedulers_parsed
	print "____________________________"
	print utilisations_parsed
	print "******************************"
	print workload_parsed
	
	exit()
	
	schedulers_parsed = [p[4] for p in parsing_3]
	
	utilisations_parsed = [p[6] for p in parsing_3]

	workload_parsed = ['_'.join(p[4] + p[5].split('.')[0]) for p in parsing_3]
	
	print utilisations_parsed, schedulers_parsed, workload_parsed
	
	exit()
	

	
	all_results = [[utilisations_parsed[i], schedulers_parsed[i], workload_parsed[i]]+parsing_1[i][1:] for i in range(len(parsing_1))]

	for i in range(len(parsing_1)):
		all_results[i].append(slr_from_file2[i])
		all_results[i].append(slr_standard_dev_from_file_2[i])
		

	print all_results[0]


	mean_slr_dict = collections.defaultdict(lambda : [])
	median_slr_dict = collections.defaultdict(lambda : [])
	min_slr_dict = collections.defaultdict(lambda : [])
	max_slr_dict = collections.defaultdict(lambda : [])
	
	
	workloads = set([res[2] for res in all_results])
	
	value_to_plot_index = -2
	
	#workloads.remove("jobs_power_independent")
	
	for w in workloads:
		
		
		for res in all_results:
			if res[2] == w:
				mean_slr_dict[(res[0], res[1])].append(res[value_to_plot_index])
		
		
	#	print "mean_slr_dict"
	#	print mean_slr_dict.items()[1:10]
		
	#	exit()
		
		ress_to_plot = collections.defaultdict(lambda : [])
		print len(mean_slr_dict.items())
		
		for i in mean_slr_dict.items():
			
			mean_mean_slrs = numpy.average([numpy.float32(j) for j in i[1]])
			
			ress_to_plot[i[0][1]].append([int(i[0][0]), mean_mean_slrs])
		
		
		colour_dict = {"projected_slr" : "b", "identity" : "g", "fifo_job" : "r"}
		
		
		print ress_to_plot.items()

		for i in ress_to_plot.items():
		
			sorted_i1 = sorted(i[1], key=lambda x: x[0])
		
			utils = [j[0] for j in sorted_i1]
			means = [j[1] for j in sorted_i1]
			plt.semilogy(utils, means, colour_dict[i[0]])
		
	
	plt.show()





	exit()



	print all_results[0:3]
	print "len all results", len(all_results)
	
	utilisations = [int(u) for u in utilisations_parsed]
	schedulers = schedulers_parsed
	
	results_interest_means = []
	results_interest_medians = []
	
	for s in schedulers:
		for u in utilisations:
			for t in ['a','b','c']:
				
				matches_means = [float(r[4]) for r in all_results if ((int(r[0]) == int(u)) and (s == r[1]) and (r[2][0] == t))]
				
				matches_medians = [float(r[6]) for r in all_results if ((int(r[0]) == int(u)) and (s == r[1]) and (r[2][0] == t))]
							
				results_interest_means.append([s, u, t, matches_means])
				results_interest_medians.append([s, u, t, matches_medians])
	
	print len(results_interest_means)
	print "expected len", len(schedulers) * len(utilisations) * 3
	
	print len(schedulers), len(utilisations)
	
	exit()
	
	all_fifo_task_means = []
	all_fifo_job_means = []
	all_fifo_task_medians = []
	all_fifo_job_medians = []
	
	for i in range(len(results_interest_means)):
		
		if results_interest_means[i][0] == "fifo_task":
			all_fifo_task_means.extend(results_interest_means[i][3])
		if results_interest_medians[i][0] == "fifo_task":
			all_fifo_task_medians.extend(results_interest_means[i][3])
		
		if results_interest_means[i][0] == "fifo_job":
			all_fifo_job_means.extend(results_interest_means[i][3])
		if results_interest_medians[i][0] == "fifo_job":
			all_fifo_job_medians.extend(results_interest_means[i][3])
			

	#print all_fifo_task_means

	all_fifo_task_means = numpy.array(all_fifo_task_means)
	all_fifo_job_means =  numpy.array(all_fifo_job_means)
	all_fifo_task_medians =  numpy.array(all_fifo_task_medians)
	all_fifo_job_medians =  numpy.array(all_fifo_job_medians)
	
	
	
	#all_fifo_task_means = numpy.array([r[3] for r in results_interest_means if r[0] == "fifo_task"])
	#all_fifo_job_means = numpy.array([r[3] for r in results_interest_means if r[0] == "fifo_job" ])
	
	#print len(all_fifo_task_means), len(all_fifo_job_means)
	
	means_ratios = all_fifo_task_means / all_fifo_job_means
	medians_ratios = all_fifo_job_medians / all_fifo_task_medians
	
	print means_ratios[0:100]
	
	
	means_ratios_by_utilisation = []
	medians_ratios_by_utilisation = []
	
	
	for u in utilisations:
		means_ratios_by_utilisation.append(\
		 [means_ratios[i] for i in range(len(means_ratios)) 
		  if (results_interest_means[int(math.floor(float(i)/workload_count))][1] == u)])
		  
		medians_ratios_by_utilisation.append(\
		 [medians_ratios[i] for i in range(len(medians_ratios)) 
		  if (results_interest_medians[int(math.floor(float(i)/workload_count))][1] == u)])
	
	
	
	matplotlib.pyplot.boxplot(medians_ratios_by_utilisation)
	matplotlib.pyplot.show()
	
	print "analysis finished"
		exit()
	
	
	all_fifo_task_by_workload_kind = {}
	all_fifo_job_by_workload_kind = {}
	
	for workload_kind in ['a','b','c']:
		all_fifo_task_by_workload_kind[workload_kind] = [r[3] for r in results_interest_means 
								 if (r[0] == "fifo_task") and (r[2] == workload_kind)]
		all_fifo_job_by_workload_kind[workload_kind] = [r[3] for r in results_interest_means 
								 if (r[0] == "fifo_job") and (r[2] == workload_kind)]
								 
	
	
	
	
	
	print len(a), len(b)
	#	print a, b
	
	ameans = [numpy.mean(x) for x in a]
	amedians = [numpy.median(x) for x in a]
	amins = [numpy.min(x) for x in a]
	amaxs = [numpy.max(x) for x in a]
	
	bmeans = [numpy.mean(x) for x in b]
	bmedians = [numpy.median(x) for x in b]
	bmins = [numpy.min(x) for x in b]
	abaxs = [numpy.max(x) for x in b]
	
	
	fig = matplotlib.pylab.figure()

	matplotlib.pylab.errorbar(utilisations, amedians)
	matplotlib.pylab.errorbar(utilisations, bmedians)
	
	
	

	matplotlib.pylab.show()
	
	sigs = [scipy.stats.mannwhitneyu(a[i],b[i])[1]<0.05 for i in range(len(a))]
	ranksums = [scipy.stats.ranksums(a[i],b[i]) for i in range(len(a))]

	print sigs, ranksums
	
	
	#matplotlib.pyplot.boxplot(a)
	#matplotlib.pyplot.boxplot(b)
	matplotlib.pyplot.show()

	"""
if __name__ == "__main__":
	#print "extracting data"
	#extract_even_newer()
	
	print "analysing and graphing data"
	analysis_even_newer()
	
	print "pairwise analysis"
	#analysis_each_schedule()

	print "analysis finished"





