from analysis import *
import cPickle as pickle
import os
import numpy
from quickwork import load_workload


def n_tile_array(arr, n, operation):
	chunks = numpy.array_split(arr, n)
	ntiles = numpy.array([operation(c) for c in chunks])
	
	return ntiles



def folder_to_short_pickle(folderpath, output_pickle_name):
	
	results_dicts = []
	
	for root, dirs, files in os.walk(folderpath):
		
		fs = files
		
	l = len(fs)
	id=1


	workload_dicts = {}


	for f in fs:
	
		print id, '/', l
		id += 1
	
	
		resf =  open( folderpath + '/' + f, "rb" )
		res_dict = pickle.load(resf)

		
		
		
		value_trans = numpy.array(res_dict["z_all_job_values"])
		
		run_workload_jobcount = len(value_trans)
		
		
		negative_values_mask = value_trans < 0
		
		timed_out = negative_values_mask
		
		res_dict["starved_count"] = numpy.count_nonzero(timed_out)
		res_dict["starved_proportion"] = float(res_dict["starved_count"]) / float(len(timed_out))
		
		value_trans[negative_values_mask] = 0
		
		res_dict["workload_total_value_nopenalty"] = numpy.sum(value_trans)
		res_dict["proportion_of_max_value_nopenalty"] = float(res_dict["workload_total_value_nopenalty"]) / float(res_dict["workload_value_maximum"])
		
		
		workload_path = res_dict["workload_filename"]
		#print workload_path
		
		if workload_path not in workload_dicts.keys():
			texec_dict, texec_array2 = load_workload(workload_path)
			workload_dicts[workload_path] = (texec_dict, texec_array2)
		else:
			texec_dict, texec_array2 = workload_dicts[workload_path]
		
		texec_array = (texec_array2[:run_workload_jobcount]).astype('float32')
		
		"""other_max = float(numpy.sum(texec_array.astype('float32')))
		
		if other_max != res_dict["workload_value_maximum"]:
			print other_max,res_dict["workload_value_maximum"]
			print "value maxima mismatch error"
			print other_max - res_dict["workload_value_maximum"]
			
			print texec_array[-10:]
			
			exit()
		"""
		value_trans_vmax_penalty = (numpy.array(res_dict["z_all_job_values"])).astype('float32')
		value_trans_half_penalty = (numpy.array(res_dict["z_all_job_values"])).astype('float32')

		"""
		for i in range(len(texec_array)):
			if texec_array[i] < value_trans_vmax_penalty[i]:
				print texec_array[i], value_trans_vmax_penalty[i]
				print 'huh??? value more than exec'
				exit()
		"""





		#print len(value_trans_vmax_penalty), len(texec_array)

		#if len(value_trans_vmax_penalty) != len(texec_array):
		#	print "array lengths don't match"
		#	exit()




		#print value_trans_vmax_penalty[:10], texec_array[:10], negative_values_mask[:10]
		
		#print len(value_trans_vmax_penalty[negative_values_mask]), len(texec_array[negative_values_mask])
		
		#c = value_trans_vmax_penalty[negative_values_mask] + texec_array[negative_values_mask]
		
		value_trans_vmax_penalty[negative_values_mask] = - texec_array[negative_values_mask]
		value_trans_half_penalty[negative_values_mask] = -0.5 *  texec_array[negative_values_mask]

		res_dict["workload_total_value_vmaxpenalty"] = numpy.sum(value_trans_vmax_penalty)
		res_dict["proportion_of_max_value_vmaxpenalty"] = float(res_dict["workload_total_value_vmaxpenalty"]) / float(res_dict["workload_value_maximum"])

		res_dict["workload_total_value_halfpenalty"] = numpy.sum(value_trans_half_penalty)
		res_dict["proportion_of_max_value_halfpenalty"] = float(res_dict["workload_total_value_halfpenalty"]) / float(res_dict["workload_value_maximum"])



		#need to work out decile distributions again


		execs_argsorted = numpy.argsort(texec_array)
		maxvals_argsorted = numpy.take(texec_array, execs_argsorted)
		values_nopenalty_argsorted = numpy.take(value_trans, execs_argsorted)
		values_vmaxpenalty_argsorted = numpy.take(value_trans_vmax_penalty, execs_argsorted)
		values_halfpenalty_argsorted = numpy.take(value_trans_half_penalty, execs_argsorted)
		starved_argsorted = numpy.take(timed_out, execs_argsorted)
		
		
		
		decile_vmax = n_tile_array(maxvals_argsorted, 10, numpy.sum)
		decile_nopenalty = n_tile_array(values_nopenalty_argsorted, 10, numpy.sum)
		decile_vmaxpenalty = n_tile_array(values_vmaxpenalty_argsorted, 10, numpy.sum)
		decile_halfpenalty = n_tile_array(values_halfpenalty_argsorted, 10, numpy.sum)
		decile_starved = n_tile_array(starved_argsorted, 10, numpy.count_nonzero)
		decile_starved_proportion = n_tile_array(starved_argsorted, 10, lambda(x): float(numpy.count_nonzero(x)) / float(len(x)))

		res_dict["decile_maxvals"] = list(decile_vmax)
		res_dict["decile_values_nopenalty"] = list(decile_nopenalty)
		res_dict["decile_values_vmaxpenalty"] = list(decile_vmaxpenalty)
		res_dict["decile_values_halfpenalty"] = list(decile_halfpenalty)
		res_dict["decile_starved"] = list(decile_starved)
		res_dict["decile_starved_proportion"] = list(decile_starved_proportion)
		

		res_dict["decile_prop_maxval_nopenalty"] = list(decile_nopenalty.astype('float32') / decile_vmax.astype('float32'))
		res_dict["decile_prop_maxval_vmaxpenalty"] = list(decile_vmaxpenalty.astype('float32') / decile_vmax.astype('float32'))
		res_dict["decile_prop_maxval_halfpenalty"] = list(decile_halfpenalty.astype('float32') / decile_vmax.astype('float32'))
		
		del res_dict["z_all_job_values"]
		del res_dict["z_all_job_slrs"]
		del res_dict["z_job_start_order"]
		
		
		#for r in sorted(res_dict.keys()):
		#	print r, res_dict[r]
		
		#print res_dict
		
		resf.close()
		results_dicts.append(res_dict)

	outf = open( output_pickle_name, "wb" )
	pickle.dump(results_dicts, outf, 2)
	outf.close()





	





def decile_mean_slr_fig(results_dicts, decile_param, utilisations, average_func, plotter, ylim=None, flip=False):
	
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
	
	
	for s in sorted(results_dicts[0].keys()):
		print s
	
	"""
	disregarded_orderers = ["pslr_sf",
							"texec_sum_value_density_down",
							"projected_value_straight_up",
							"pvd_sq_down",
							"projected_value_density_down",
							"projected_value_remaining_up"
							
							]
	
	"""
	cluster_orderers = sorted(set([r["cluster_orderer"] for r in results_dicts]))# - set(disregarded_orderers))




	print cluster_orderers


	fig = plt.figure()
	ax = fig.add_subplot(111)

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




	for ord in sorted(cluster_orderers):

		relevant_samples = [
			r[decile_param]
			for r in results_dicts
			if (r["target_utilisation"] in utilisations) and (r["cluster_orderer"] == ord)# and (r["timeouts_enabled"] == True)
			]



		relevant_samples = numpy.array(relevant_samples)


		x = len(relevant_samples[0])


		rs_avgs = [average_func(relevant_samples[:,ind])
				   for ind in range(x)]


		plotter([xi+1 for xi in range(x)], rs_avgs, label=scheduler_renames[ord], marker=tick_dict[ord],  color=colour_dict[ord])




	#plt.legend(loc='upper right')
	
	
	text_replacement_dict = {"mean_slr" : "Mean SLR",
		"worst case slr" : "Median of worst-case Job SLR Values",
		"worst_case_slr" : "Median of worst-case Job SLR Values",
		"communication_to_computation_ratio" : "Communication to Computation Ratio",
		"gini_coeff_of_slrs" : "Median Gini Coefficient of Job SLR Values",
		"exec_time_estimate_value" : "Base for log rounding inaccurate estimator",
		"proportion_of_max_value": "Proportion of maximum value",
		"target_utilisation" : "Load",
		"decile_prop_maxval_nopenalty" : "Proportion of maximum value (no penalties applied)",
		"decile_prop_maxval_vmaxpenalty" : "Proportion of maximum value (with penalties)",
		"decile_starved_proportion": "Percentage of jobs incomplete by final deadline",
		"decile_slr_means": "Mean SLR for Decile",
		"decile_worst_slr": "Worst-Case SLR for Decile"}
		
	
	


	"""if x_parameter in text_replacement_dict.keys():
		xtext = text_replacement_dict[x_parameter]
	else:
		xtext = x_parameter
	"""
	
	if decile_param in text_replacement_dict.keys():
		ytext = text_replacement_dict[decile_param]
	else:
		ytext = decile_param

	plt.xticks(fontsize=18)
	plt.yticks(fontsize=18)

	plt.xlabel("Decile of job execution time", fontsize=16)
	plt.ylabel(text_replacement_dict[decile_param], rotation="vertical", fontsize=16)

	if ylim != None:
		ax.set_autoscaley_on(False)
		ax.set_ylim(ylim)

	if flip:
		plt.gca().invert_yaxis()

	#plt.legend(loc='upper right')

	plt.show()

	a = random.uniform(1,100000)

	filename = str(utilisations[0]) + '_' + decile_param + '_' + str(a) + '.pdf'




	fig.savefig(chart_output_path + filename, format='pdf')



def linetimeouts(results_dicts, average_func, plotter):
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



	for s in sorted(results_dicts[0].keys()):
		print s
	
	
	disregarded_orderers = ["pslr_sf",
							"texec_sum_value_density_down",
							"projected_value_straight_up",
							"pvd_sq_down",
					#		"projected_value_density_down",
							"projected_value_remaining_up"
							
							]
							
							
	cluster_orderers = sorted(set([r["cluster_orderer"] for r in results_dicts]) - set(disregarded_orderers))

							
	print cluster_orderers
	
	
	fig = plt.figure()
	ax = fig.add_subplot(111)
	


	results = collections.defaultdict(lambda:[])



	for ord in cluster_orderers:
		
		
		
		these_rs = collections.defaultdict(lambda:[])
		
		
		relevant_samples = [
							(r["target_utilisation"], 100.0 * (float(r["timeout_count"]) / float(r["max_job_count"])))
							for r in results_dicts
							if (r["cluster_orderer"] == ord) and (r["timeouts_enabled"] == True)
							]
							
		for r in relevant_samples:
			these_rs[r[0]].append(r[1])
			
		xs = []
		ys = []
			
		for item in sorted(these_rs.items(), key=lambda x: x[0]):

			xs.append(item[0])
			ys.append(average_func(numpy.array(item[1])))


									   
		plotter(xs, ys, label=ord, marker=tick_dict[ord],  color=colour_dict[ord])


	plt.legend(loc='upper left')

	plt.show()


	a = random.uniform(1,100000)

	filename = str('timeout proportion util.pdf')
	fig.savefig(chart_output_path + filename, format='pdf')



def new_thesis_analysis(results_dicts_file_path, inac_transform = False):

	print 'loading pickle'
	results_dicts = pickle.load( open( results_dicts_file_path, "rb" ) )
	print len(results_dicts), "pickled run dicts loaded"
	

	for k in sorted(results_dicts[0].keys()):
		print k, results_dicts[0][k]

	

	print set([r["target_utilisation"] for r in results_dicts])
	print set([r["exec_time_estimate_kind"] for r in results_dicts])


	#exit()
	
	for r in sorted(results_dicts[0].keys()):
		print r


	ss = collections.defaultdict(lambda:0)
	
	for r in results_dicts:
		ss[r['cluster_orderer']] += 1

	for s in sorted(ss.keys()):
		print s, ':', ss[s]


	#exit()

	#inac_transform = False
	print 'do you need to remember to switch inac transforming back on for inac results'

	if inac_transform:
		print 'inac transforming...'
		exacts = [r for r in results_dicts if r["exec_time_estimate_kind"] == "exact"]

		c1 = copy.deepcopy(exacts)
		c2 = copy.deepcopy(exacts)

		for id in range(len(c1)):
			c1[id]["exec_time_estimate_kind"] = "log_round"
			c1[id]["exec_time_estimate_value"] = 0
			c2[id]["exec_time_estimate_kind"] = "normal_jitter"
			c2[id]["exec_time_estimate_value"] = 0

		results_dicts.extend(c1)
		results_dicts.extend(c2)




	filt = True
			
	if filt:
		print 'filtering'
		
		disregarded_orderers =  ["pslr_sf",
								"texec_sum_value_density_down",
								"projected_value_straight_up",
								"pvd_sq_down",
								"projected_value_density_down",
								"projected_value_remaining_up",
								
								#next ones just for seven and network results
								#"pvd_sq_up",
								#"projected_value_straight_down",
								#"texec_sum_value_density_up",
								#"projected_value_density_up",
								#"projected_value_remaining_down"
								]
		
		
		include_orderers = [
		"projected_slr",
		#"pslr_sf",
		"shortest_remaining_first",
		"projected_value_straight_down",
		"projected_value_remaining_down",
		"pvd_sq_up"
		]
		
		
		
		#results_dicts = [r for r in results_dicts if (r["timeouts_enabled"] == False) and (r["cluster_orderer"] not in disregarded_orderers)]
		#results_dicts = [r for r in results_dicts if (r["exec_time_estimate_kind"] == 'log_round') and (r["target_utilisation"] == 100) and (r["cluster_orderer"] not in disregarded_orderers)]


		#r2 = [r for r in results_dicts if (r["exec_time_estimate_kind"] == 'exact') and (r["target_utilisation"] == 100) and (r["cluster_orderer"] not in disregarded_orderers)]
		
		
		#print len(r2)
		
		#exit()

		#for r in r2:
		#	r["exec_time_estimate_kind"] = 'log_round'
		#	r["exec_time_estimate_value"] = 0
				
		#results_dicts = results_dicts + r2
		
		
		
		
						 #and (r["target_utilisation"] == 90))]
		results_dicts = [r for r in results_dicts if
						 #True
						 r["timeouts_enabled"]			== False
						 and r["cluster_orderer"] not in disregarded_orderers
						 #and r["cluster_orderer"]  in include_orderers
						 #and r["target_utilisation"]		== 110
						 #and r["exec_time_estimate_kind"]	== "log_round"
						 #and r["exec_time_estimate_kind"]	== "normal_jitter"
						 #and r["arrival_pattern"]			== "day_week_pattern"
						 #and r["exec_time_estimate_value"]	== 100
						 ]

	correct_ccr = True
	if correct_ccr:
		results_dicts = correct_ccr_in_results_dicts(results_dicts)
	


	print len(results_dicts)

	if len(results_dicts) == 0:
		print "filters too tight: no results"
		exit()


	schedulers = set([r['cluster_orderer'] for r in results_dicts])

	print 'drawing...'
	
	#for s in sorted(schedulers):
	#	print '"' + s + '": "" ,'

	#print 'schedulers', schedulers

	#exit()

	#playing


	#fn = lambda p: 1 - (numpy.mean(numpy.array(p)))
	#line_plot(results_dicts, "target_utilisation", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, fn, None, plt.semilogy)


	#f = lambda x : 1- numpy.median(x)

	#line_plot(results_dicts, "exec_time_estimate_value", "proportion_of_max_value", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot)#, yaxislimit=[0.7,1.0])

	#line_plot(results_dicts, "communication_to_computation_ratio", "proportion_of_max_value", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot)#, yaxislimit=[0.7,1.0])

	#line_plot(results_dicts, "exec_time_estimate_value", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot)#, yaxislimit=[0.7,1.0])


	#decile_mean_slr_fig(results_dicts, "decile_prop_maxval_nopenalty", [80], lambda y: 100.0*numpy.mean(y), plt.plot, flip=True)#, ylim=(-0.5,100.0))

	#linetimeouts(results_dicts, numpy.mean, plt.semilogy)

	#line_plot(results_dicts, "target_utilisation", "starved_proportion", "cluster_orderer", {}, {}, lambda y: 100.0 * numpy.mean(y), None, semilogy)#, yaxislimit=(0.01,100.0))

	#line_plot(results_dicts, "target_utilisation", "tardy_propotion", "cluster_orderer", {}, {}, lambda y: 100.0 * numpy.mean(y), None, plt.semilogy)#, yaxislimit=(0.01,100.0))








	#final utilisation (utilisation_six.pik)

	#decile_mean_slr_fig(results_dicts, "decile_prop_maxval_nopenalty", [120], numpy.mean, plt.plot)
	#decile_mean_slr_fig(results_dicts, "decile_prop_maxval_nopenalty", [120], numpy.mean, plt.plot, ylim=(0.60,1.0))
	#decile_mean_slr_fig(results_dicts, "decile_prop_maxval_vmaxpenalty", [120], numpy.mean, plt.plot)
	#decile_mean_slr_fig(results_dicts, "decile_prop_maxval_vmaxpenalty", [120], numpy.mean, plt.plot, ylim=(0.50,1.0))

	#line_plot(results_dicts, "target_utilisation", "proportion_of_max_value_nopenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot)#, yaxislimit=(0.70,1.0))
	#line_plot(results_dicts, "target_utilisation", "proportion_of_max_value_nopenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, yaxislimit=(0.70,1.0))
	#line_plot(results_dicts, "target_utilisation", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot)#, yaxislimit=(0.70,1.0))
	#line_plot(results_dicts, "target_utilisation", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, yaxislimit=(0.70,1.0))


	#final utilisation (utilisation_seven.pik)

	#decile_mean_slr_fig(results_dicts, "decile_starved_proportion", [120], lambda y: 100.0*numpy.mean(y), plt.plot, flip=True, ylim=(-0.5,100.0))
	#decile_mean_slr_fig(results_dicts, "decile_starved_proportion", [120], lambda y: 100.0*numpy.mean(y), plt.plot, flip=True, ylim=(-0.5,8.0))
	#line_plot(results_dicts, "target_utilisation", "starved_proportion", "cluster_orderer", {}, {}, lambda y: 100.0 * numpy.mean(y), None, semilogy, yaxislimit=(0.01,100.0))




	#final for inacs (innacurateestimates_six.pik)
	#remember to set filtering appropriately on line 553/554 for first or second set

	#line_plot(results_dicts, "exec_time_estimate_value", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, ylabel="Base for log rounding inaccurate estimator")#, yaxislimit=[0.7,1.0])
	#line_plot(results_dicts, "exec_time_estimate_value", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, yaxislimit=[0.93,1.0], ylabel="Base for log rounding inaccurate estimator")
	#line_plot(results_dicts, "exec_time_estimate_value", "proportion_of_max_value_nopenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, ylabel="Base for log rounding inaccurate estimator")#, yaxislimit=[0.7,1.0])
	#line_plot(results_dicts, "exec_time_estimate_value", "proportion_of_max_value_nopenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, yaxislimit=[0.93,1.0], ylabel="Base for log rounding inaccurate estimator")

	#line_plot(results_dicts, "exec_time_estimate_value", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, ylabel="Standard deviation for normal distribution inaccurate estimator")#, yaxislimit=[0.7,1.0])
	#line_plot(results_dicts, "exec_time_estimate_value", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, yaxislimit=[0.965,1.0], ylabel="Standard deviation for normal distribution inaccurate estimator")
	#line_plot(results_dicts, "exec_time_estimate_value", "proportion_of_max_value_nopenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, ylabel="Standard deviation for normal distribution inaccurate estimator")#, yaxislimit=[0.7,1.0])
	#line_plot(results_dicts, "exec_time_estimate_value", "proportion_of_max_value_nopenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, yaxislimit=[0.973,1.0], ylabel="Standard deviation for normal distribution inaccurate estimator")


	#final for networking (networking_six.pik)
	#line_plot(results_dicts, "communication_to_computation_ratio", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot)#, yaxislimit=[0.7,1.0])
	#line_plot(results_dicts, "communication_to_computation_ratio", "proportion_of_max_value_nopenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot)#, yaxislimit=[0.7,1.0])
	#line_plot(results_dicts, "communication_to_computation_ratio", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, yaxislimit=[0.95,1.0])
	#line_plot(results_dicts, "communication_to_computation_ratio", "proportion_of_max_value_nopenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, yaxislimit=[0.95,1.0])


	#post viva replots
	#line_plot(results_dicts, "target_utilisation", "proportion_of_max_value_nopenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, yaxislimit=(0.73,1.0), xaxislimit=[90.0,120.0])
	#line_plot(results_dicts, "target_utilisation", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot)#, yaxislimit=(0.70,1.0))
	#line_plot(results_dicts, "target_utilisation", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, yaxislimit=(0.73,1.0), xaxislimit=[90.0,120.0])

	#line_plot(results_dicts, "communication_to_computation_ratio", "proportion_of_max_value", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot)#, yaxislimit=[0.7,1.0])
	#line_plot(results_dicts, "communication_to_computation_ratio", "proportion_of_max_value", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, yaxislimit=[0.95,1.0])

	#utilisation_seven
	#line_plot(results_dicts, "communication_to_computation_ratio", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot)#, yaxislimit=[0.7,1.0])
	#line_plot(results_dicts, "communication_to_computation_ratio", "proportion_of_max_value_nopenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot)#, yaxislimit=[0.7,1.0])
	#line_plot(results_dicts, "communication_to_computation_ratio", "proportion_of_max_value_vmaxpenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, yaxislimit=[0.95,1.0])
	#line_plot(results_dicts, "communication_to_computation_ratio", "proportion_of_max_value_nopenalty", "cluster_orderer", {}, {}, numpy.mean, None, plt.plot, yaxislimit=[0.95,1.0])

	#synthetic chapter 6 netf_anal analysis pickled
	#line_plot(results_dicts, "communication_to_computation_ratio", "worst case slr", "cluster_orderer", {"router_allocator" : 'random'}, {}, numpy.median, None, plt.semilogy)
	#line_plot(results_dicts, "communication_to_computation_ratio", "gini_coeff_of_slrs", "cluster_orderer", {"router_allocator" : 'random'}, {}, numpy.median, None, plt.plot)

	#redone network with networking seven
	#line_plot(results_dicts, "communication_to_computation_ratio", "worst_case_slr", "cluster_orderer", {}, {}, numpy.median, None, plt.semilogy)
	#line_plot(results_dicts, "communication_to_computation_ratio", "gini_coeff_of_slrs", "cluster_orderer", {}, {}, numpy.median, None, plt.plot)

	#utilisation seven
	#decile_mean_slr_fig(results_dicts, "decile_slr_means", [120], numpy.mean, plt.semilogy)
	#decile_mean_slr_fig(results_dicts, "decile_worst_slr", [120], numpy.mean, plt.semilogy)
	decile_mean_slr_fig(results_dicts, "decile_slr_means", [120], numpy.median, plt.semilogy)
	decile_mean_slr_fig(results_dicts, "decile_worst_slr", [120], numpy.median, plt.semilogy)



def rerun_orig_analysis(results_dicts_file_path):
						
						
	results_dicts = pickle.load( open( results_dicts_file_path, "rb" ) )
	decile_mean_slr_fig(results_dicts, "decile_slr_means", [120], numpy.mean, plt.semilogy)

	meanf = lambda x: float(sum(x)) / float(len(x))


	#line_plot(results_dicts, "target_utilisation", "worst case slr", "cluster_orderer", {}, {}, numpy.median, None, plt.semilogy)


def folder_to_short_pickle_pool(num):
	if num == 0:
		folder_to_short_pickle("/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/utilisation_sweep_pickles_old1", "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/final_results_short/utilisation_seven.pik")
	if num == 1:
		folder_to_short_pickle("/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/netfinal_pickles_old1", "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/final_results_short/networking_seven.pik")
	if num == 2:
		folder_to_short_pickle("/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/inacfinal_pickles_old1", "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/final_results_short/inaccurateestimates_seven.pik")

#old, bad network delay calculator
def real_network_delay_from_ccr(exec_time, ccr):
	if not ccr > 0.0:
		print "error, computation to communication ratio cannot be 0 in this model (there will always be some computation). please try again with a positive value, however arbitrarily small)"
		exit()
	elif ccr > 1.0:
		print "error, computation to communication ratio cannot be greater than 1!"
		exit()

    #print 'exec', exec_time
    #print 'ccr', ccr
    
    
	network_delay = (float(exec_time) / float(ccr)) * (1.0 - float(ccr))
    
    #print 'netdel', network_delay
    
	return network_delay


def correct_ccr_in_results_dicts(results_dicts):


	new_rs = []
	for r in results_dicts:
		old_ccr = r["communication_to_computation_ratio"]
		#print 'old ccr', old_ccr
		if old_ccr > 0.0:
			new_net = real_network_delay_from_ccr(10.0, old_ccr)
			new_ccr = new_net / 10.0
			#print 'new ccr', new_ccr
			r["communication_to_computation_ratio"] = new_ccr
		if r["communication_to_computation_ratio"] <= 2.5:
			new_rs.append(r)



	return new_rs



	
if __name__ == "__main__":

	#folder_to_short_pickle("/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/utilisation_sweep_pickles_old1", "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/final_results_short/utilisation_seven.pik")



	#from multiprocessing import Pool
	#p = Pool(4)
	#p.map(folder_to_short_pickle_pool, [0,1,2])


	#new_thesis_analysis("/Volumes/Red 500 GB/final_results_short/utilisation.pik")
	#new_thesis_analysis("/Users/andie/Documents/Work/final_results_short_nopenalty/networkingthree.pik")
	#new_thesis_analysis("/Users/andie/Documents/Work/final_results_short_nopenalty/inaccurateestimatesthree.pik")
	#new_thesis_analysis("/Users/andie/Documents/Work/final_results_short_nopenalty/utilisation_six.pik")

	#new_thesis_analysis("/Users/andie/Documents/Work/final_results_short_nopenalty/utilisation_seven.pik")

	#re-drawing graphs post viva for corrections
	#new_thesis_analysis("/Users/andie/Documents/Work/simulation run results used in thesis/value results, chapter 7/seven/utilisation_seven.pik")
	#new_thesis_analysis("/Users/andie/Documents/Work/simulation run results used in thesis/value results, chapter 7/seven/networking_seven.pik")
	#new_thesis_analysis("/Users/andie/Documents/Work/simulation run results used in thesis/synthetic workloads results, chapter 6/netf_anal/analysis_pickled")

	#final

	#rerun_orig_analysis("/Users/andie/Documents/Work/journal_paper_results/charts_5/analysis_pickled")
	#rerun_orig_analysis("/Users/andie/Documents/Work/airbus_workload_new_analysis/analysis_pickled")

	#new_thesis_analysis("/Users/andie/Documents/Work/final_results_short_nopenalty/utilisation_six.pik")
	#new_thesis_analysis("/Users/andie/Documents/Work/final_results_short_nopenalty/inaccurateestimates_six.pik", inac_transform = True)
	#new_thesis_analysis("/Users/andie/Documents/Work/final_results_short_nopenalty/networking_six.pik")
















