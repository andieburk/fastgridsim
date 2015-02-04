""" metrics.py

"""

from GiniCoef import calc_gini5
import numpy
import collections
import cPickle as pickle
from value_calculations import actual_value_calculator
from task_state_new import TaskState
"""

classes:

metrics manager:
takes the calls from the gridsim source, takes in all the information necessary


job metric calculator - a collection of a lot of functions to calculate job metrics

workload metric calculator - a collection of a lot of functions to calculate workload metrics





"""
class MetricException(Exception):
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return repr(self.value)




class metrics_manager(object):
	def __init__(self, application_man, platform_man, network_manager, sim_params, sim_finish_time, sim_start_time=0):
		self.sim_params = sim_params
		self.job_metrics_calc = job_metrics(sim_params, network_manager)
		self.global_metrics_calculator = global_metrics(application_man, platform_man, network_manager, sim_params, sim_finish_time, sim_start_time)
		
		
		


	def runlog_schedule_output(self, schedule_filename):
		schedulefile = open(schedule_filename, 'w')
		
		jmc = self.job_metrics_calc
		
		schedulefile.write("JobName,SubmitTime,StartTime,FinishTime,SLR,CriticalPathTime,TotalCPUTicks,Stretch,Speedup,\n")
		
		for j in self.joblist:
		
			out_bits = []
			
			out_bits.append(j.name)
			out_bits.append(str(jmc.job_submission_time(j)))
			out_bits.append(str(jmc.job_start_executing_time(j)))
			out_bits.append(str(jmc.job_finish_executing_time(j)))
			out_bits.append(str(jmc.slr(j)))
			out_bits.append(str(jmc.job_critical_path_length(j)))
			out_bits.append(str(jmc.job_sum_of_task_exec_times(j)))
			out_bits.append(str(jmc.stretch(j)))
			out_bits.append(str(jmc.speedup(j)))			
		
			
			out_str = ','.join(out_bits)
			schedulefile.write(out_str + "\n")
		
		schedulefile.close()


	
	def create_global_metrics_dict(self):
		result_dict = self.sim_params.copy()
		
		metrics = self.global_metrics_calculator
		
		result_dict["job_finish_counter"] = None
		
		"""
		res1 = [self.job_metrics_calc.slr(j) for j in sorted(self.joblist, key = lambda x: self.job_metrics_calc.job_sum_of_task_exec_times(x))]
		res2 = [float(self.job_metrics_calc.job_in_flight_time(j))/float(self.job_metrics_calc.job_critical_path_length(j)) for j in self.joblist]
		
		slrs_unsorted = [self.job_metrics_calc.slr(j) for j in self.joblist]
				
		res1a = numpy.array(res1)
		res2a = numpy.array(res2)
		
		msd_stretch = self.mean_stddev_stretch()
		msd_speedup = self.mean_stddev_speedup()
		"""	
		
		if metrics.num_jobs > 10:
			
			quartile_slr_means		 = metrics.n_tile_means(metrics.slrs_sorted_by_exectime, 4)
			decile_slr_means		 = metrics.n_tile_means(metrics.slrs_sorted_by_exectime, 10)
			
			decile_slr_worstcase	 = metrics.n_tile_maxs(metrics.slrs_sorted_by_exectime, 10)
			
			quartile_slr_stddevs	 = metrics.n_tile_stddevs(metrics.slrs_sorted_by_exectime, 4)
			decile_slr_stddevs		 = metrics.n_tile_stddevs(metrics.slrs_sorted_by_exectime, 10)
			
			decile_timeouts			 = metrics.n_tile_sums(metrics.timeout_val_by_exectime, 10)
			decile_timeouts_not_end  = metrics.n_tile_sums(metrics.timed_out_val_not_end, 10)
			decile_counts			 = metrics.n_tile_lens(metrics.timeout_val_by_exectime, 10)
			
			dta = numpy.array(decile_timeouts)
			dtnea = numpy.array(decile_timeouts_not_end)
			dtca = numpy.array(decile_counts).astype(float)
			
			
			
			decile_value			= metrics.n_tile_sums(metrics.values_by_exectime, 10)
			decile_max_value		= metrics.n_tile_sums(metrics.max_val_by_exectime, 10)
			decile_value_proportion = list(numpy.array(decile_value) / numpy.array(decile_max_value))
			
			
			

			
			result_dict["decile_timeout_proportion"] = dta/dtca
			result_dict["decile_timeouts_not_end_proportion"] = dtnea/dtca
			
			result_dict["quartile_slr_means"]		= quartile_slr_means
			result_dict["decile_slr_means"]			= decile_slr_means
			result_dict["quartile_slr_stddevs"]		= quartile_slr_stddevs
			result_dict["decile_slr_stddevs"]		= decile_slr_stddevs
			result_dict["decile_worst_slr"]			= decile_slr_worstcase
			result_dict["decile_timeouts"]			= decile_timeouts
			result_dict["decile_timeouts_not_end"]	= decile_timeouts_not_end

			result_dict["decile_value"]				= decile_value
			result_dict["decile_max_value"]			= decile_max_value
			result_dict["decile_value_proportion"]	= decile_value_proportion
		
		
		
		
		result_dict["mean_slr"]						= numpy.average(metrics.slrs)
		result_dict["minimum_slr"]					= numpy.amin(metrics.slrs)
		result_dict["median_slr"]					= numpy.median(metrics.slrs)
		result_dict["maximum_slr"]					= numpy.amax(metrics.slrs)
		result_dict["stddev_slr"]					= numpy.std(metrics.slrs)
		result_dict["mean_in_flight_over_cp"]		= numpy.average(metrics.in_flight_over_cps)
		result_dict["minimum_in_flight_over_cp"]	= numpy.amin(metrics.in_flight_over_cps)
		result_dict["median_in_flight_over_cp"]		= numpy.median(metrics.in_flight_over_cps)
		result_dict["maximum_in_flight_over_cp"]	= numpy.amax(metrics.in_flight_over_cps)
		result_dict["stddev_in_flight_over_cp"]		= numpy.std(metrics.in_flight_over_cps)
		result_dict["workload_makespan"]			= metrics.workload_makespan()
		result_dict["average_actual_utilisation"]	= metrics.cluster_total_utilisation()
		result_dict["flow"]							= metrics.flow()
		result_dict["peak_in_flight"]				= metrics.peak_in_flight()
		result_dict["mean_stretch"]					= numpy.average(metrics.stretches)
		result_dict["stddev_stretch"]				= numpy.std(metrics.stretches)#msd_stretch[1]
		result_dict["mean_speedup"]					= numpy.average(metrics.speedups)#msd_speedup[0]
		result_dict["stddev_speedup"]				= numpy.std(metrics.speedups)#msd_speedup[1]
		#result_dict["z_all job speedups"]			= msd_speedup[2]
		result_dict["cumulative_completion"]		= metrics.cumulative_completion_generator()
		result_dict["gini_coeff_of_slrs"]			= metrics.gini_coefficient_slr()
		result_dict["gini_coeff_of_slr_inverses"]	= metrics.gini_coefficient_slr(True)
		result_dict["jain_fairness_of_slrs"]		= metrics.jain_fairness_slr()
		result_dict["jain_fairness_of_slr_inverses"]= metrics.jain_fairness_slr(True)
		result_dict["sum_of_excess_slrs"]			= metrics.sum_of_excess_slrs()
		
		result_dict["cumulative_completion_with_base"] = metrics.cumulative_completion_generator(base = self.sim_params["cumulative_completion_base"])
		result_dict["z_all_job_slrs"]				= metrics.slrs
		result_dict["worst_case_slr"]				= numpy.amax(metrics.slrs)
		result_dict["z_all_job_values"]				= metrics.values
		result_dict["workload_total_value"]			= metrics.total_workload_value()
		result_dict["value_over_time"]				= metrics.total_workload_value_over_time()
		result_dict["z_job_start_order"]			= metrics.job_names_by_start_order
		
		result_dict["timeout_count"]				= metrics.timed_out_job_count
		result_dict["timeout_wasted_time"]			= metrics.timeouts_wasted_time
		result_dict["proportion_of_max_value"]		= metrics.proportion_of_max_val_achieved
		result_dict["workload_value_maximum"]		= numpy.sum(metrics.max_val)
				
		result_dict["tardy_count"]					= metrics.tardy_count
		result_dict["tardy_propotion"]				= metrics.tardy_propotion
	

		
		result_dict["router_allocator"]		= result_dict["router_allocator"].name
		result_dict["router_orderer"]		= result_dict["router_orderer"].name
		result_dict["cluster_allocator"]	= result_dict["cluster_allocator"].name
		result_dict["cluster_orderer"]		= result_dict["cluster_orderer"].name
		
		return result_dict
	
	
	def print_global_metrics_all(self):
		res_dict = self.create_global_metrics_dict()
		
		print sorted(self.sim_params.keys())
		print self.sim_params["metrics_out_formats"]
		print "pickle" in self.sim_params["metrics_out_formats"]
		
		if "metrics_out_formats" in self.sim_params.keys():
		
			if "text_machine" in self.sim_params["metrics_out_formats"]:
				self.print_global_metrics_machine_readable(res_dict)
			
			if "text_human" in self.sim_params["metrics_out_formats"]:
				self.print_global_metrics_human_readable(res_dict)
	
			if ("pickled" in self.sim_params["metrics_out_formats"]) or ("pickle" in self.sim_params["metrics_out_formats"]):
				self.output_pickled(res_dict)
	
		elif "human_readable_metrics" in self.sim_params.keys():
			if self.sim_params["human_readable_metrics"]:
				self.print_global_metrics_human_readable(res_dict)
				
		else:
			self.print_global_metrics_machine_readable(res_dict)


	def print_global_metrics_machine_readable(self, res_dict):
		line_key = "#Results_Output#:#Values#"
		values = [line_key]
		for i in res_dict.items():
			
			#print str(type(i[1]))
			#print "class" in str(type(i[1]))
			
			if "class" in str(type(i[1])):
				istr = ''.join([i[0], ":", str(i[1].name), ":", "str"])
			else:
				tstr = str(type(i[1])).split("'")[1]
				istr = ''.join([i[0], ":", str(i[1]), ":", tstr])
			values.append(istr)
		
		outstr = '|'.join(values)
		
		print outstr
		
		
		
	
	def print_global_metrics_human_readable(self, res_dict):
		for i in sorted(res_dict.items(), key = lambda x : x[0]):
			print i[0], ":", i[1]



	def output_pickled(self, res_dict):
		
		print 'outputting pickled file'
		
		#generate pickle filename from run param file unique id? "pickle runlog for param file x"

		if "pickle_output_path" in self.sim_params.keys():
			pickle_output_path = self.sim_params["pickle_output_path"]
		else:
			pickle_output_path = ""

		output_pickle_filename = "pick_runlog_for_" + self.sim_params["params_filename"] + ".bin"

		output_pickle_filepath = pickle_output_path + output_pickle_filename

		#pickle the results dict
		with open(output_pickle_filepath, "wb") as pickle_outfile:
			pickle.dump(res_dict, pickle_outfile, protocol=2)

		print 'pickle file output to', output_pickle_filepath



class job_metrics(object):
	def __init__(self, sim_params, network_manager = None):
		self.sim_params = sim_params
		self.network_manager = network_manager

	def job_submission_time(self, job):
		return job.submit_time

	def job_start_executing_time(self, job):
		return min([t.starttime for t in job.tasklist])
	
	def job_finish_executing_time(self, job):
		return max([t.finishtime for t in job.tasklist])
	
	def job_in_flight_time(self, job):
		return self.job_finish_executing_time(job) - self.job_start_executing_time(job)
		
	def job_submit_to_finish_time(self, job):
		return self.job_finish_executing_time(job) - self.job_submission_time(job)

	def job_sum_of_task_exec_times(self, job):
		return sum([t.imp_cores_fast[False][True] for t in job.tasklist])

	def job_turnaround_over_critical_path_ratio(self, job):
	
		slr = float(self.job_submit_to_finish_time(job))/float(self.job_critical_path_length(job))
		if slr < 1.0:
			print "error: calculated SLR is less than 1 for job", job.name
			print slr
			print self.job_submit_to_finish_time(job)
			print self.job_critical_path_length(job)
			
			#print self.network_manager.down_exec_time_with_network_delay(job, False)
			#print self.network_manager.up_exec_time_with_network_delay(job, False)
			
			print [t.get_execution_time() for t in job.tasklist]
			print [t.requirements_kind for t in job.tasklist]
			print [[ti.taskid for ti in t.get_dependents()] for t in job.tasklist]
			print [[ti.taskid for ti in t.get_dependencies()] for t in job.tasklist]
			
			job.print_job()
			raise MetricException('here', slr)
		return slr
	
	def job_in_flight_over_critical_path_ratio(self, job):
		return float(self.job_in_flight_time(job))/float(self.job_critical_path_length(job))
	
	def job_critical_path_length(self, job):
		if self.sim_params["networking_enabled"]:
			critical_path, path_parts = self.network_manager.up_exec_time_with_network_delay(job)
			return critical_path
		else:
			return self.down_exec_time(job)

	def task_dependents_exec_time_sum(self, task):
		if len(task.get_dependents()) > 0:
			return task.get_execution_time() + sum([self.task_dependents_exec_time_max(t) for t in task.get_dependents()])
		else:
			return task.get_execution_time()
	
	def task_dependents_exec_time_max(self, task):
		if len(task.get_dependents()) > 0:
			return task.get_execution_time() + max([self.task_dependents_exec_time_max(t) for t in task.get_dependents()])
		else:
			return task.get_execution_time()

	def down_exec_time(self, job, estimate = False): #aka critical path length
		#assumes tasklist is topologically sorted. This is OK because the workload file reader assumes this too.
		cumulative_times = {}
		for t in job.tasklist:
			
			if len(t.get_dependencies()) > 0:
				cumu_sum_times = max([cumulative_times[t2] for t2 in t.get_dependencies()])
			else:
				cumu_sum_times = 0
			cumulative_times[t] = t.get_execution_time(estimate) + cumu_sum_times
		
		longest_critical_path_from_any_task = max(cumulative_times.values())
		
		return longest_critical_path_from_any_task
	
	def up_exec_time(self, job, estimate = False, sum_instead = False):
		#topcuoglu's upward rank for dependent tasks.
		#assumes tasklist is topologically sorted.
		if not self.sim_params["networking_enabled"]:
		
			task_up_exec_times = {}
			
			for t in reversed(job.tasklist):
				
				if len(t.get_dependents()) > 0:
					if not sum_instead:
						up_max = max([task_up_exec_times[t2.taskid] for t2 in t.get_dependents()])
					else:
						up_max = sum([task_up_exec_times[t2.taskid] for t2 in t.get_dependents()])
				else:
					up_max = 0
				task_up_exec_times[t.taskid] = t.get_execution_time(estimate) + up_max

			if not sum_instead:
				critical_path = max(task_up_exec_times.values())
			else:
				critical_path = sum(task_up_exec_times.values())
		
			return critical_path, task_up_exec_times
		else:
			#networking enabled
			critical_path, path_parts = self.network_manager.up_exec_time_with_network_delay(job, estimate)
			return critical_path, path_parts

	
	def stretch(self, job):
		return float(self.job_submit_to_finish_time(job)) / float(self.job_sum_of_task_exec_times(job))
	
	def speedup(self, job):
		return float(self.job_sum_of_task_exec_times(job)) / float(self.job_submit_to_finish_time(job))
	
	def slr(self,job):
		return self.job_turnaround_over_critical_path_ratio(job)

	def job_final_value(self, job):
		if self.sim_params["value_enabled"]:
			value_curve = job.parent_application_manager.curvesdict[job.value_curve_name]
			
			time = self.job_finish_executing_time(job)
			
			if job.timed_out:
				job_slr = 10.0
			else:
				job_slr = self.slr(job)
			
			value = actual_value_calculator(value_curve, job, time, job_slr)
			if self.sim_params["debugging_on"]:
				print "actual value for", job.name, "at", time, "with slr", job_slr, "is", value
			return value
		else:
			return 0



	def job_maximum_value(self, job):
		if self.sim_params["value_enabled"]:
			value_curve = job.parent_application_manager.curvesdict[job.value_curve_name]
			
			time = self.job_finish_executing_time(job)
			#job_slr = self.slr(job)
			
			value = actual_value_calculator(value_curve, job, time, 1.0)
			if self.sim_params["debugging_on"]:
				print "max theoretical value for", job.name, "at", time, "with slr", 1.0, "is", value
			return value
		else:
			return 0
				
	def job_timeout_wasted_time(self, job):
		if job.timed_out:
			return sum([t.get_execution_time() * t.corecount for t in job.tasklist
						if t.state_manager.state == TaskState.completed])
		else:
			return 0

	def job_timeout_count(self, job):
		if job.timed_out:
			return 1
		else:
			
			ts = set([t.state_manager.state for t in job.tasklist])
			if ts != set([3]):
				print "error with timeouts?"
				print job.name
				print ts
				exit()
			
			return 0

	def job_timeout_count_not_end(self, job):
		if job.timed_out and (not job.timed_out_at_end):
			return 1
		else:
			return 0





class global_metrics(object):
	def __init__(self, application_man, platform_man, network_manager, sim_params, sim_finish_time, sim_start_time=0):
		self.sim_params = sim_params
		self.job_metrics_calc = job_metrics(sim_params, network_manager)
		
		completed_jobs = [j for j in application_man.joblist if not j.timed_out]
		self.timed_out_job_count = len([j for j in application_man.joblist if j.timed_out])
		
		self.joblist_inc_timedout = application_man.joblist
		self.joblist = completed_jobs
		self.platman = platform_man
		
		self.sim_start = sim_start_time
		self.sim_finish = sim_finish_time
		
		self.job_metrics = self.job_metrics_calc
		
		self.calculate_job_metrics_vectors(sim_start_time, sim_finish_time)

		
		if "print_schedule" in sim_params.keys():
			if sim_params["print_schedule"]:
				self.print_schedule()


				

	def print_schedule(self):
		print "schedule"

		job_starts = [(self.job_metrics_calc.job_start_executing_time(j), j.name) for j in self.joblist]
		jobs_starts_sorted = sorted(job_starts, key = lambda x: x[0])
		for x in jobs_starts_sorted:
			print x
		
	def calculate_job_metrics_vectors(self, sim_start_time, sim_finish_time):
		
		job_metric_vector_apply				 = lambda x : numpy.array([x(j) for j in self.joblist],				 dtype=numpy.uint64)
		job_metric_vector_apply_posneg		 = lambda x : numpy.array([x(j) for j in self.joblist],				 dtype=numpy.float32)
		job_metric_vector_apply_inc_timeouts = lambda x : numpy.array([x(j) for j in self.joblist_inc_timedout], dtype=numpy.uint64)
		jmva_all							 = lambda x : numpy.array([x(j) for j in self.joblist_inc_timedout], dtype=numpy.float32)

			
		self.num_jobs = len(self.joblist)

		self.t_core_seconds   = job_metric_vector_apply(self.job_metrics_calc.job_sum_of_task_exec_times)
		self.t_core_seconds_t = job_metric_vector_apply_inc_timeouts(self.job_metrics_calc.job_sum_of_task_exec_times)
		self.submits		  = job_metric_vector_apply(self.job_metrics_calc.job_submission_time)
		self.starts			  = job_metric_vector_apply(self.job_metrics_calc.job_start_executing_time)
		self.finishes		  = job_metric_vector_apply(self.job_metrics_calc.job_finish_executing_time)
		self.critical_paths   = job_metric_vector_apply(self.job_metrics_calc.job_critical_path_length)
		self.values			  = jmva_all(self.job_metrics_calc.job_final_value)
		
		
		self.timed_out_val  = job_metric_vector_apply_inc_timeouts(self.job_metrics_calc.job_timeout_count)
		self.timed_out_val_not_end = job_metric_vector_apply_inc_timeouts(self.job_metrics_calc.job_timeout_count_not_end)
		self.timeouts_wasted_time = sum(job_metric_vector_apply_inc_timeouts(self.job_metrics_calc.job_timeout_wasted_time))
		
		
		self.timeout_proportion_all = float(numpy.sum(self.timed_out_val)) / self.num_jobs
		self.timeout_proportion_not_end = float(numpy.sum(self.timed_out_val_not_end)) / self.num_jobs
		
		self.max_val = jmva_all(self.job_metrics_calc.job_maximum_value)
		self.proportion_of_max_val_achieved = numpy.sum(self.values) / numpy.sum(self.max_val)
		
		self.pends = self.starts - self.submits
		self.in_flights = self.finishes - self.starts
		self.turnarounds = self.finishes - self.submits
		
		cps_as_float = self.critical_paths.astype(numpy.float32)
		
		self.slrs = self.turnarounds.astype(numpy.float32) / cps_as_float
		self.in_flight_over_cps = self.in_flights.astype(numpy.float32) / cps_as_float
		self.speedups = self.turnarounds.astype(numpy.float32) / self.t_core_seconds.astype(numpy.float32)
		self.stretches = 1.0 / self.speedups
		
		self.tardy_count = numpy.sum(self.slrs > 1.0) + self.timeout_proportion_all
		self.tardy_propotion = float(self.tardy_count) / self.num_jobs
		
		exec_time_indices = numpy.argsort(self.t_core_seconds)

		if len(exec_time_indices) == len(self.slrs):
			self.slrs_sorted_by_exectime = numpy.take(self.slrs, exec_time_indices)
		else:
			print "length mismatch 1", len(exec_time_indices), len(self.slrs)
			exit()

		exec_time_indices_t = numpy.argsort(self.t_core_seconds_t)
		if len(exec_time_indices_t) == len(self.timed_out_val):
			self.timeout_val_by_exectime = numpy.take(self.timed_out_val, exec_time_indices_t)
			self.max_val_by_exectime = numpy.take(self.max_val, exec_time_indices_t)
			self.values_by_exectime = numpy.take(self.values, exec_time_indices_t)
		else:
			print "length mismatch 2", len(exec_time_indices_t), len(self.timed_out_val)
			exit()


		self.sim_start_time = self.sim_start_time_calc(sim_start_time)
		self.sim_finish_time = self.sim_finish_time_calc(sim_finish_time)

		self.job_names = [j.name for j in self.joblist]
		starts_argsorted = numpy.argsort(self.starts)
		self.job_names_by_start_order = numpy.take(self.job_names, starts_argsorted)
	
	
	
	
	
	
	
	
	def sim_start_time_calc(self, given_sim_start):
		sim_start_time_simpy = given_sim_start
		sim_start_time_jobs = numpy.min(self.submits)
		sim_start_time_tasks = min([min([t.starttime for t in j.tasklist]) for j in self.joblist])
		
		timeset = set([sim_start_time_simpy, sim_start_time_jobs, sim_start_time_tasks])
		
		if len(timeset) > 1:
			print "warning: start times do not match", timeset
			#print sorted([([t.starttime for t in j.tasklist]) for j in self.joblist])
			#print [(j.submit_time, min([t.starttime for t in j.tasklist])) for j in self.joblist]
			#print self.joblist[0].submit_time
			#print min([t.starttime for t in self.joblist[0].tasklist])
						
			#exit()
			return min(timeset)
		
		return sim_start_time_simpy
	
	def sim_finish_time_calc(self, given_sim_finish):
		sim_finish_time_simpy = given_sim_finish
		sim_finish_time_jobs = numpy.max(self.finishes)
		sim_finish_time_tasks = max([max([t.finishtime for t in j.tasklist]) for j in self.joblist])

		timeset = set([sim_finish_time_simpy, sim_finish_time_jobs, sim_finish_time_tasks])
		if len(timeset) > 1:
			print "error: finish times do not match", timeset
	
		return sim_finish_time_simpy
		
	def cluster_total_utilisation(self):
		etimesum =  numpy.sum(self.t_core_seconds)
		#sum([self.job_metrics_calc.job_sum_of_task_exec_times(j) for j in self.joblist])
		return 100.0 * (float(etimesum)/(len(self.platman.resources) * (self.sim_finish_time+1 - self.sim_start_time)))
	
	def flow(self):
		return float(len(self.joblist)) / float(self.sim_finish_time - self.sim_start_time)
	
	def workload_makespan(self):
		return self.sim_finish_time - self.sim_start_time
	
	def peak_in_flight(self):
		#inflightarr = numpy.zeros(shape=(self.sim_finish_time-self.sim_start_time))
		
		
		job_start_times = self.starts
		job_end_times = self.finishes
		
		time_happen_array = []
		
		for jst in job_start_times:
			time_happen_array.append((jst, 1))
		for jet in job_end_times:
			time_happen_array.append((jet, -1))
		
		#print time_happen_array
		
		sorted_times = sorted(time_happen_array, key=lambda x: x[0] + x[1]*0.1)
		
		current_count = 0
		max_count = 0
		
		for st in sorted_times:
			current_count += st[1]
			if current_count > max_count:
				max_count = current_count
		
		return max_count
		
	
	def cumulative_completion_generator(self, base = None):
		if base == None:
			base = self.sim_finish_time
		jmetrics = self.job_metrics
		
		cumul_by_job = (((base + 1) - jmetrics.job_finish_executing_time(j)) * jmetrics.job_sum_of_task_exec_times(j)
						for j in self.joblist)
		
		cumul_comp = sum(cumul_by_job)
		
		return cumul_comp
		

	
	def utilisation_other(self):
		used_cycles = sum([sum([t.get_execution_time() for t in j.tasklist]) for j in self.joblist])
		print 'utilisation', 100.0 * (float(used_cycles) / (float(len(self.platman.resources)) * float(self.sim_finish_time+1-self.sim_start_time)))
	

	"""
	def mean_stddev_stretch(self):
		#jmetrics = self.job_metrics
		stretches = self.stretches
		meanstretch = numpy.mean(stretches)
		stddevstretch = numpy.std(stretches)
		return (meanstretch, stddevstretch)
	
	def mean_stddev_speedup(self):
		#jmetrics = self.job_metrics
		speedups = self.speedups#[jmetrics.speedup(j) for j in self.joblist]
		meanspeedup = numpy.mean(speedups)
		stddevspeedup = numpy.std(speedups)
		return (meanspeedup, stddevspeedup, self.speedups)
		
	def mean_stddev_slr(self):
		#jmetrics = self.job_metrics
		stretches = [jmetrics.slr(j) for j in self.joblist]
		meanstretch = numpy.mean(stretches)
		stddevstretch = numpy.std(stretches)	
		return (meanstretch, stddevstretch, stretches)
		
	"""
		
	def gini_coefficient_slr(self, inverse = False):
		#jmetrics = self.job_metrics
		if not inverse:
			slrs = self.slrs
		else:
			slrs = 1.0 / self.slrs# [1.0/jmetrics.slr(j) for j in self.joblist]
		gini = calc_gini5(slrs)
		return gini
	
	def jain_fairness_slr(self, inverse = False):
		#jmetrics = self.job_metrics
		if inverse:
			values_to_consider = slrs = 1.0 / self.slrs#[1.0/jmetrics.slr(j) for j in self.joblist]
		else:
			values_to_consider = slrs = self.slrs
		
		top = numpy.sum(values_to_consider) ** 2
		bottom = numpy.sum(numpy.square(values_to_consider))
		num = len(values_to_consider)
		
		jain = float(top) / (float(num) * float(bottom))
		
		return jain
	
	
	def sum_of_excess_slrs(self):
		#jmetrics = self.job_metrics
		#return sum((jmetrics.slr(j) - 1.0 for j in self.joblist))
		return numpy.sum(self.slrs - 1.0)

	def n_tile_array(self, arr, n, operation):
		chunks = numpy.array_split(arr, n)
		ntiles = [operation(c) for c in chunks]
				
		return ntiles
	
	
	def n_tile_lens(self,array,n):
		return self.n_tile_array(array, n, len)
	
	
	def n_tile_means(self, array, n):
		return self.n_tile_array(array, n, numpy.average)
	
	def n_tile_sums(self, array, n):
		
		#print len(array)
		
		return self.n_tile_array(array, n, numpy.sum)
	
	def n_tile_maxs(self, array, n):
		return self.n_tile_array(array, n, numpy.amax)
	
	def n_tile_stddevs(self, array, n):
		return self.n_tile_array(array, n, numpy.std)

	def total_workload_value(self):
		return numpy.sum(self.values)

	def total_workload_value_over_time(self):
		return self.total_workload_value() / (self.sim_finish - self.sim_start)




#old code ===========================================================

"""
		
		#for jid in range(len(job_start_times)):		
		#	inflightarr[job_start_times[jid]:job_end_times[jid]] += 1 
		
		#return numpy.max(inflightarr)

	
	def cumulative_completion(self):
		#cum_completion_arr = numpy.zeros(shape=(self.sim_finish_time-self.sim_start_time))
		jmetrics = job_metrics()
		
		
		
		job_end_execs = [(max([t.finishtime for t in j.tasklist]), jmetrics.job_sum_of_task_exec_times(j)) for j in self.joblist]
		
		sorted_job_end_execs = sorted(job_end_execs, key=lambda x : x[0])
		
		sje_arr = numpy.array(sorted_job_end_execs)
		
		#print sje_arr[0:20]
		#print len(sje_arr)

		
		time_increases = {} #collections.defaultdict(lambda : 0L)
		
		for s in sorted_job_end_execs:
			if s[0] in time_increases:
				time_increases[s[0]] += s[1]
			else:
				time_increases[s[0]] = s[1]
		
			#time_increases[s[0]] += s[1]
		
		print "time increases", time_increases
		
		time_inc_sorted = sorted(time_increases.items(), key=lambda x:x[0])
		
		
		curr_cumulative_completion = 0L
		curr_total_cumulative = 0L
		
		for time_inc_sorted_id in xrange(1, len(time_inc_sorted)):
		
			ticks_between_last_ranges = time_inc_sorted[time_inc_sorted_id][0] - time_inc_sorted[time_inc_sorted_id - 1][0]
			
			print time_inc_sorted[time_inc_sorted_id][0]
			
			curr_cumulative_completion += curr_total_cumulative * long(ticks_between_last_ranges)
			curr_total_cumulative += long(time_inc_sorted[time_inc_sorted_id][1])
			
		return curr_cumulative_completion
	
	def cumulative_completion_better(self):
		
		#this gives an error - cumulative completion tends to be a very large number and so overflows the numpy int64.
		
		#sim_start_time = 
		#sim_finish_time = 
		#self.sim_finish_time - self.sim_start_time
		
		jmetrics = job_metrics()
		
		
		job_finish_times_array = numpy.array([numpy.int64(jmetrics.job_finish_executing_time(j)) for j in self.joblist])
		job_texec_times_array = numpy.array([numpy.int64(jmetrics.job_sum_of_task_exec_times(j)) for j in self.joblist])
		
		cumul_completion_last_tick = numpy.int64(self.sim_finish_time + 1)
		
		seconds_for_cumulating_by_job = cumul_completion_last_tick - job_finish_times_array
		
		cumul_for_each_job = seconds_for_cumulating_by_job * job_texec_times_array

		cumulative_comp = numpy.sum(cumul_for_each_job)
		
		return cumulative_comp
	


	
	#def sum_of_excess_pending_seconds(self):
	#	jmetrics = self.job_metrics
		
	

		
		
		time_inc_sorted_id = 0
		next_time_inc = time_inc_sorted[time_inc_sorted_id][0]
		#print time_increases.items()[0:20], len(time_increases)
		
		
		curr_cumulative_completion = 0L
		curr_total_cumulative = 0L
		
		
		
		
		#print "exiting cum comppletion calcs"
		#exit()
		
		#limit = 2**256
		
		for time_tick in xrange(self.sim_start_time, self.sim_finish_time+1):
			
			print time_inc_sorted_id
			print next_time_inc
			print time_inc_sorted[0:20]
			
			exit()
		
		
			if time_tick == next_time_inc:
				curr_total_cumulative += time_inc_sorted[time_tick][1]
				time_inc_sorted_id += 1
				next_time_inc = time_inc_sorted[time_inc_sorted_id][0]
			
			
		#	if curr_cumulative_completion > limit:
		#		print "limit exceeded"
		#		exit()
			
			curr_cumulative_completion += curr_total_cumulative
			#print time_tick, curr_total_cumulative, curr_cumulative_completion
		
		#print "cc", curr_cumulative_completion
		return curr_cumulative_completion
		
		
		
		
		
		
		
		#job_end_times = [max([t.finishtime for t in j.tasklist]) for j in self.joblist]
		
		#job_exec_times = [jmetrics.job_sum_of_task_exec_times(j) for j in self.joblist]
		
		
		#for jid in range(len(job_end_times)):
		#	cum_completion_arr[job_end_times[jid]:] += job_exec_times[jid]
		
		#print cum_completion_arr
			
		#return numpy.sum(cum_completion_arr) + sum(job_exec_times)
		"""
	


	
"""	
	def print_global_metrics_old(self):
		
		all_turnaround_ratios = [self.job_metrics_calc.job_turnaround_over_critical_path_ratio(j) for j in self.joblist]
		#print all_turnaround_ratios
		print 'of all turnaround ratios in this run: min, median, mean, max: ', min(all_turnaround_ratios), numpy.median(all_turnaround_ratios), numpy.average(all_turnaround_ratios), max(all_turnaround_ratios)
		print 'cluster utilisation', self.cluster_total_utilisation()
		self.utilisation_other()
		
		#short_res = []
		#for j in self.joblist:
		#	short_res.append(self.job_submit_to_finish_time(j))
		#	short_res.append(self.job_in_flight_time(j))
		#	short_res.append(self.job_critical_path_length(j))
		#print short_res
		
		print "workload makespan", self.workload_makespan()
		print "average utilisation", self.cluster_total_utilisation()
		print "flow", self.flow()
		print "peak in flight", self.peak_in_flight()
		print "mean stddev stretch", self.mean_stddev_stretch()
		print "mean stddev speedup", self.mean_stddev_speedup()
		print "mean stddev slr", self.mean_stddev_slr()		
		print "cumulative completion", self.cumulative_completion()
		
		
		

		res1 = [self.job_metrics_calc.slr(j) for j in sorted(self.joblist, key = lambda x: self.job_metrics_calc.job_sum_of_task_exec_times(x))]
		res2 = [float(self.job_metrics_calc.job_in_flight_time(j))/float(self.job_metrics_calc.job_critical_path_length(j)) for j in self.joblist]
		#print res1
		#print res2
		#print numpy.average(res1)
		#print numpy.average(res2)
		
		res1a = numpy.array(res1)
		res2a = numpy.array(res2)
		
		res = [	self.sim_params["target_utilisation"],\
				numpy.average(res1a),\
				numpy.amin(res1a),\
				numpy.median(res1a),\
				numpy.amax(res1a),\
				numpy.average(res2a),\
				numpy.amin(res2a),\
				numpy.median(res2a),\
				numpy.amax(res2a)\
			  ]
		
		res_str = [str(r) for r in res]
		print '#-#,' , ','.join(res_str)
		#for j in self.joblist:
		#	print j.name, self.job_metrics_calc.job_submission_time(j), self.job_metrics_calc.job_start_executing_time(j), self.job_metrics_calc.job_finish_executing_time(j)
		
		
		
		
		#numpy.average(res2)',', numpy.median(res1), ',', numpy.median(res2)
		#print '#-# , ',utilisation_value, ',', numpy.median(res1), ',', numpy.median(res2)
		
		
		workloadsum = sum([self.job_metrics_calc.job_sum_of_task_exec_times(j) for j in self.joblist])
		print 'sum of all exec times' , workloadsum
		
		
		msd_stretch = self.mean_stddev_stretch()
		msd_speedup = self.mean_stddev_speedup()

		quartile_slr_means	= self.n_tile_means(res1a, 4)
		decile_slr_means	= self.n_tile_means(res1a, 10)
		
		
		print quartile_slr_means
		exit()
		
		result_dict = self.sim_params.copy()
		
		result_dict["mean_slr"]						= numpy.average(res1a)
		result_dict["minimum_slr"]					= numpy.amin(res1a)
		result_dict["median_slr"]					= numpy.median(res1a)
		result_dict["maximum_slr"]					= numpy.amax(res1a)
		result_dict["mean_in_flight_over_cp"]		= numpy.average(res2a)
		result_dict["minimum_in_flight_over_cp"]	= numpy.amin(res2a)
		result_dict["median_in_flight_over_cp"]		= numpy.median(res2a)
		result_dict["maximum_in_flight_over_cp"]	= numpy.amax(res2a)
		result_dict["workload makespan"]			= self.workload_makespan()
		result_dict["average actual utilisation"]	= self.cluster_total_utilisation()
		result_dict["flow"]							= self.flow()
		result_dict["peak in flight"]				= self.peak_in_flight()
		result_dict["mean stretch"]					= msd_stretch[0]
		result_dict["stddev stretch"]				= msd_stretch[1]
		result_dict["mean speedup"]					= msd_speedup[0]
		result_dict["stddev speedup"]				= msd_speedup[1]
		result_dict["cumulative completion"]		= self.cumulative_completion()
		result_dict["quartile_slr_means"]			= self.cumulative_completion()
		
		
		print result_dict
		
		
"""		
		
