"""schedulers.py

"""

from import_manager import import_manager
import_manager_local = import_manager()
if import_manager_local.import_trace:
	from SimPy.SimulationTrace import *
else:
	from SimPy.Simulation import *

if import_manager_local.use_pypy:
	import numpypy as numpy
else:
	import numpy

import random
from metrics import job_metrics
from value_calculations import read_curves_file, value_curve_valid, actual_value_calculator, potential_positive_value_remaining, value_calculator_vectored



class SchedulingError(Exception):
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return repr(self.value)



class identity_orderer(object):
	def __init__(self):
		self.name = "identity"
		#print "WARNING: as tasklists are sets - simply using the identity orderer is now the same as using the random orderer!"

	def ordered(self, ls):
		return ls
	
	def ordered_jobs(self, ls):
		return ls

class random_orderer(object):
	def __init__(self):
		self.name = "random"

	def ordered(self, ls):
		random.shuffle(ls)
		return ls
	
	def ordered_jobs(self, ls):
		random.shuffle(ls)
		return ls

class fifo_task_orderer(object):
	def __init__(self):
		self.name = "fifo_task"
		
	def ordered(self, ls):
		sortfunc = lambda t: t.ready_order_id
		return sorted(ls, key=sortfunc)
	
	def ordered_jobs(self, ls):
		return ls


class fifo_job_orderer(object):
	def __init__(self):
		self.name = "fifo_job"

	def ordered_jobs(self, ls):
		sortfunc = lambda j : j.job_ready_id
		ls_sorted = sorted(ls, key=sortfunc)

		if len(ls) != len(ls_sorted):
			print "big problem"
			exit()
		
		return ls_sorted

		#smallest_job_id = min([j.name for j in ls])
		#return [j for j in ls if j.name == smallest_job_id] #updating this working here
	
	def task_id(self, task):
		return task.taskid
	
	def ordered(self,ls):
		#print [t.taskid for t in ls]
		#print [t.taskid for t in sorted(ls, key=self.task_id)]
		return ls #sorted(ls, key=lambda t: t.parent_job.job_ready_id * 10000000 + t.ready_order_id)




"""




class projected_slr_orderer(object):
	def __init__(self):
		self.name = "projected_slr"
		print "creating a proj slr orderer"
		self.workload_loaded = False
		self.debugging_on = True
		self.backward = False
		

		
		
		
		
	def initialise_with_workload(self, simulation_parameters, joblist, network_manager = None):
		self.debugging_on = simulation_parameters["debugging_on"]
		if "p_slr_dir" in simulation_parameters.keys():
			self.backward = simulation_parameters["p_slr_back"]
		
		if "networking_enabled" in simulation_parameters.keys():
			if simulation_parameters["networking_enabled"]:
				if network_manager == None:
					print "networking enabled but no network manager supplied to projected_slr orderer"
					exit()
		
		self.workload_loaded = True

		self.job_critical_path = {}
		self.job_submit = {}
		self.task_sub_critical_path = {}
		
		
		numtasks = sum([len(j.tasklist) for j in joblist])
				
		self.task_subs_array = numpy.ones(numtasks)
		self.job_cps_array = numpy.ones(numtasks)
		self.job_submits_array = numpy.ones(numtasks)
		
		taskcounter = 0
		self.taskid_to_index = {}
		
		job_metric_calculator = job_metrics(simulation_parameters, network_manager)
		
		for j in joblist:
			self.job_submit[j.name] = j.submit_time
			jcp, tcsps = job_metric_calculator.up_exec_time(j, estimate = True)
			self.job_critical_path[j.name] = jcp #add the job critical path to the job critical path dictionary
			self.task_sub_critical_path.update(tcsps) #add the task remaining critical path to do to the task cp dicitionary
			
			for t in j.tasklist:
				
				self.task_subs_array[taskcounter] = numpy.float64(self.task_sub_critical_path[t.taskid])
				self.job_cps_array[taskcounter] = numpy.float64(self.job_critical_path[t.parent_job.name])
				self.job_submits_array[taskcounter] = numpy.float64(self.job_submit[t.parent_job.name])
				
				self.taskid_to_index[t.taskid] = taskcounter
				taskcounter += 1
				
		if self.debugging_on:
			print "SLR orderer initialised"
		
			print len(self.task_subs_array)
			print self.task_subs_array[0:100]
			print self.job_cps_array[0:100]
			print self.job_submits_array[0:100]
		

	def ordered_jobs(self, ls):
		if self.debugging_on:
			if not self.workload_loaded:
				print "error: workload has not yet been loaded for the SLR orderer - jobs"
				exit()
			print "-- SLR job orderer ordering this list:", [j.name for j in ls], "at", now()
			
		
		if len(ls) <= 1:
			return ls
		
		job_cps = []
		job_submits = []
		now_np = numpy.float64(now()+1) #massively interesting fudge factor - does this help? !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		
		for j in ls:
			job_cps.append(numpy.float64(self.job_critical_path[j.name]))
			job_submits.append(numpy.float64(self.job_submit[j.name]))

		job_critical_path_array = numpy.array(job_cps)
		job_submits_array = numpy.array(job_submits)
		
		job_pred_finish_time = now_np + job_critical_path_array
		predicted_SLR = (job_pred_finish_time - job_submits_array) / job_critical_path_array
		
		indices = numpy.argsort(predicted_SLR)
		sorted_jobs_ascending = numpy.take(ls, indices)
		sorted_jobs = sorted_jobs_ascending[::-1]
		
		if self.debugging_on:
			sorted_slrs_ascending = numpy.take(predicted_SLR, indices)
			sorted_slrs = sorted_slrs_ascending[::-1]
			print "SLR job order given:", [(sorted_jobs[id].name, round(sorted_slrs[id], 5)) for id in range(len(ls))]
		
		return sorted_jobs
	
	
	def from_list_one(self, predicted_SLR, ls):
		task_and_pslrs = [(ls[i], predicted_SLR[i]) for i in range(len(ls))]
		sorted_tasks_and_pslrs = sorted(task_and_pslrs, key=lambda x: x[1], reverse = True)
		sorted_tasks = [s[0] for s in sorted_tasks_and_pslrs]
		return sorted_tasks
	
	def from_list_two(self, predicted_SLR, ls):
		task_and_pslrs = [(predicted_SLR[i], ls[i]) for i in range(len(ls))]
		sorted_tasks_and_pslrs = sorted(task_and_pslrs, reverse = True)
		sorted_tasks = [s[1] for s in sorted_tasks_and_pslrs]	
		return sorted_tasks
	
	def from_list_three(self, predicted_SLR, ls):
		task_and_pslrs = zip(predicted_SLR, ls) #[(predicted_SLR[i], ls[i]) for i in range(len(ls))]
		task_and_pslrs.sort(reverse=True)
		
		#sorted_pslrs, sorted_tasks = zip(*task_and_pslrs) #[s[1] for s in task_and_pslrs]	
		sorted_tasks = [s[1] for s in task_and_pslrs]	
		return sorted_tasks	
	
	
	def from_list_four(self, predicted_SLR, ls):
		task_and_pslrs = zip(predicted_SLR, range(len(ls)))
		task_and_pslrs.sort(reverse=True)
	
		sorted_tasks = [ls[i[1]] for i in task_and_pslrs]
		
		return sorted_tasks
	
	
	
	
	
	
	
	
	def from_list_numpy(self, predicted_SLR, ls):
		indices = numpy.argsort(predicted_SLR)
		sorted_tasks_ascending = numpy.take(ls, indices)
		sorted_tasks = sorted_tasks_ascending[::-1]			
		return sorted_tasks
	
	def from_list_numpy_two(self, predicted_SLR, ls):
		indices = numpy.argsort(predicted_SLR)[::-1]
		sorted_tasks_ascending = numpy.take(ls, indices)
		#sorted_tasks = sorted_tasks_ascending	
		return sorted_tasks_ascending		
	
	def calc_arrays_old(self, ls):
		task_subs = []
		job_cps = []
		job_submits = []

		
		for t in ls:
			task_subs.append(numpy.float64(self.task_sub_critical_path[t.taskid]))
			job_cps.append(numpy.float64(self.job_critical_path[t.parent_job.name]))
			job_submits.append(numpy.float64(self.job_submit[t.parent_job.name]))
		
		task_sub_critical_path_array = numpy.array(task_subs)
		job_critical_path_array = numpy.array(job_cps)
		job_submits_array = numpy.array(job_submits)
		
		return task_sub_critical_path_array, job_critical_path_array, job_submits_array
	
	def calc_arrays_numpy_pypy(self, ls):
		task_indices = set([self.taskid_to_index[t.taskid] for t in ls])
		task_sub_critical_path_array = numpy.array([self.task_subs_array[t] for t in task_indices])
		job_critical_path_array = numpy.array([self.job_cps_array[t] for t in task_indices])
		job_submits_array = numpy.array([self.job_submits_array[t] for t in task_indices])

		return task_sub_critical_path_array, job_critical_path_array, job_submits_array
	
	
	def calc_arrays_numpy_real(self, ls):
		task_indices = numpy.array([self.taskid_to_index[t.taskid] for t in ls])
		task_sub_critical_path_array = self.task_subs_array[task_indices]
		job_critical_path_array = self.job_cps_array[task_indices]
		job_submits_array = self.job_submits_array[task_indices]

		return task_sub_critical_path_array, job_critical_path_array, job_submits_array
	
	 
	def ordered(self, ls): #the core of the projected_slr orderer ******************
		#for a list of tasks

		
		if self.debugging_on:
			if not self.workload_loaded:
				print "error: workload has not yet been loaded for the SLR orderer - tasks"
				exit()
			print "-- SLR task orderer ordering this list:", [t.taskid for t in ls], "at", now()
		
		if len(ls) <= 1:
			return ls
		
		now_np = numpy.float64(now()+1) #here's that nasty fudge factor again. helps to distinguish between short/long when many jobs being scheduled at their submit time		
	
		if import_manager_local.use_pypy:
			task_sub_critical_path_array, job_critical_path_array, job_submits_array =\
				self.calc_arrays_numpy_pypy(ls)
		else:
			task_sub_critical_path_array, job_critical_path_array, job_submits_array =\
				self.calc_arrays_numpy_real(ls)
				
		predicted_finish_time_array = now_np + task_sub_critical_path_array
		predicted_SLR = (predicted_finish_time_array - job_submits_array) / job_critical_path_array

		#for tasks not on the critical path (and on a dep chain tail that doesn't link back to the CP) - the predicted SLR could be below 1
		#this isn't a problem. those will only get run if the tasks that are on the CP have been run first. as time passes with not being
		#run, they'll rise up to having an SLR above 1 anyway
		
		if import_manager_local.use_pypy:
			sorted_tasks = self.from_list_one(predicted_SLR, ls)
		else:
			sorted_tasks = self.from_list_four(predicted_SLR, ls)
		
		
		if self.debugging_on:
			#sorted_slrs_ascending = numpy.take(predicted_SLR, indices)
			#sorted_slrs = sorted_slrs_ascending[::-1]
			
			#print "SLR task order given:", [(sorted_tasks[id].taskid, round(sorted_slrs[id], 5)) for id in range(len(ls))]
			pass
		
		return sorted_tasks

"""


class shortest_remaining_first_orderer(object):
	def __init__(self):
		self.name = "shortest_remaining_first"
		print "\t\tcreating a shortest remaining first orderer"
		self.workload_loaded = False
		self.debugging_on = True
		self.backward = False
	
	
	def initialise_with_workload(self, simulation_parameters, joblist, network_manager = None):
		self.debugging_on = simulation_parameters["debugging_on"]
		
		if "networking_enabled" in simulation_parameters.keys():
			if simulation_parameters["networking_enabled"]:
				if network_manager == None:
					print "networking enabled but no network manager supplied to projected_slr orderer"
					exit()
		
		
		
		self.workload_loaded = True
		
		self.job_critical_path = {}
		self.job_submit = {}
		self.task_sub_critical_path = {}
		job_metric_calculator = job_metrics(simulation_parameters, network_manager)
		
		for j in joblist:
			self.job_submit[j.name] = j.submit_time
			jcp, tcsps = job_metric_calculator.up_exec_time(j, estimate = True)
			self.job_critical_path[j.name] = jcp #add the job critical path to the job critical path dictionary
			self.task_sub_critical_path.update(tcsps) #add the task remaining critical path to do to the task cp dicitionary
		
		if self.debugging_on:
			print "\t\tshortest remaining orderer initialised"

	def ordered(self, ls):
		#for a list of tasks
		if self.debugging_on:
			if not self.workload_loaded:
				print "error: workload has not yet been loaded for the srtf orderer - tasks"
				exit()
			print "-- srtf task orderer ordering this list:", [t.taskid for t in ls], "at", now()
		
		if len(ls) <= 1:
			return ls
		
		sortfunc = lambda t: self.task_sub_critical_path[t.taskid]
		
		srtf_sorted_tasklist = sorted(ls, key=sortfunc)		
		
		return srtf_sorted_tasklist

	def ordered_jobs(self, ls):
		sortfunc = lambda j : self.job_critical_path[j.name]
		srtf_sorted_joblist = sorted(ls, key=sortfunc)
		return srtf_sorted_joblist


class longest_remaining_first_orderer(shortest_remaining_first_orderer):
	
	def ordered_jobs(self, ls):
		self.name = "longest_remaining_first"
		sortfunc = lambda j : self.job_critical_path[j.name]
		srtf_sorted_joblist = sorted(ls, key=sortfunc, reverse=True)
		return srtf_sorted_joblist

	def ordered(self, ls):
		self.name = "longest_remaining_first"
		#for a list of tasks
		if self.debugging_on:
			if not self.workload_loaded:
				print "error: workload has not yet been loaded for the lrtf orderer - tasks"
				exit()
			print "-- lrtf task orderer ordering this list:", [t.taskid for t in ls], "at", now()
		
		if len(ls) <= 1:
			return ls
		
		sortfunc = lambda t: self.task_sub_critical_path[t.taskid]
		
		srtf_sorted_tasklist = sorted(ls, key=sortfunc, reverse=True)
		
		return srtf_sorted_tasklist	


"""
	def __init__(self):
		self.name = "longest_remaining_first"
		print "\t\tcreating a longest remaining first orderer"
		self.workload_loaded = False
		self.debugging_on = True
		self.backward = False
	
	
	def initialise_with_workload(self, simulation_parameters, joblist, network_manager = None):
		self.debugging_on = simulation_parameters["debugging_on"]
		
		if "networking_enabled" in simulation_parameters.keys():
			if simulation_parameters["networking_enabled"]:
				if network_manager == None:
					print "networking enabled but no network manager supplied to projected_slr orderer"
					exit()
		
		self.workload_loaded = True
		
		self.job_critical_path = {}
		self.job_submit = {}
		self.task_sub_critical_path = {}
		job_metric_calculator = job_metrics(simulation_parameters, network_manager)
		
		for j in joblist:
			self.job_submit[j.name] = j.submit_time
			jcp, tcsps = job_metric_calculator.up_exec_time(j)
			self.job_critical_path[j.name] = jcp #add the job critical path to the job critical path dictionary
			self.task_sub_critical_path.update(tcsps) #add the task remaining critical path to do to the task cp dicitionary
		
		if self.debugging_on:
			print "\t\tlongest remaining orderer initialised"
	
	def ordered(self, ls):
		#for a list of tasks
		if self.debugging_on:
			if not self.workload_loaded:
				print "error: workload has not yet been loaded for the lrtf orderer - tasks"
				exit()
			print "-- lrtf task orderer ordering this list:", [t.taskid for t in ls], "at", now()
		
		if len(ls) <= 1:
			return ls
		
		sortfunc = lambda t: self.task_sub_critical_path[t.taskid]
		
		srtf_sorted_tasklist = sorted(ls, key=sortfunc, reverse=True)		
		
		return srtf_sorted_tasklist
	
	def ordered_jobs(self, ls):
		sortfunc = lambda j : self.job_critical_path[j.name]
		srtf_sorted_joblist = sorted(ls, key=sortfunc, reverse=True)
		return srtf_sorted_joblist

random
first_free
earliest_start_time
load_balancing
first_free_better
longest_remaining_first
"""


class random_allocator(object):
	def __init__(self):
		self.name = "random"

	def best_for_task(self, task, corecount, free_resources):
		return random.sample(free_resources, corecount)
	
	def best_for_job(self, job, resources):
		return random.choice(resources)


class first_free_allocator(object):
	def __init__(self):
		self.name = "first_free"
	def best_for_task(self, task, corecount, free_resources):
		return free_resources[0:corecount]
	
	def best_for_job(self, job, resources):
		return resources[0]


class earliest_start_time_allocator(object):
	def __init__(self):
		self.name = "earliest_start_time"
	def best_for_task(self, task, corecount, free_resources):
		pass
		
	def best_for_job(self, job, resources):
		pass

class load_balancing_allocator(object):
	def __init__(self):
		self.name = "load_balancing"

	def best_for_task(self, task, corecount, free_resources):
		print "error: best_for_task unimplemented in load_balancing_allocator - load balancing doesn't make sense at task allocation level"
		exit()
		
	def best_for_job(self, job, resources):
		
		sr_loads = [self.sub_resource_loads(r, job) for r in resources]
		
		loads_sorted = sorted(sr_loads, key=lambda x: x[1])
		
		if job.debugging_on:
			if loads_sorted[0][1] != min([s[1] for s in loads_sorted]):
				print "error: load balancer not returning cluster with lowest load"
				print "returning value", loads_sorted[0][0].name, loads_sorted[0][1]
				print "list", [(s[0].name, s[1]) for s in loads_sorted]
				exit()
				
		return loads_sorted[0][0]
	
	def sub_resource_loads(self, resource, job):
		if resource.simulation_object_type == "cluster":
			all_tasks_would_be_in_new_list = []
			all_tasks_would_be_in_new_list.extend(resource.tasklist)
			all_tasks_would_be_in_new_list.extend(job.tasklist)
			
			return (resource, sum([t.imp_cores_fast[True][True] for t in all_tasks_would_be_in_new_list]) / float(resource.get_cluster_core_count()))
		elif resource.simulation_object_type == "router":
			sub_loads = [self.sub_resource_loads(this_sub, job)[1] for this_sub in resource.sublist] 
			return (resource, min(sub_loads))
		
		
		
	
		



class first_free_better(object):
	def __init__(self):
		self.name = "first_free_better"

	def best_for_task(self, task, corecount, free_resources):
		return free_resources[0:corecount]

	def best_for_job(self, job, resources):
		pass
		#return resources[0]
	#working here


def apply_function_to_joblist(function, ls, projected_parts_dict):

		curvesdict = ls[0].parent_application_manager.curvesdict
		projected_value_function = lambda i: function(
													 curvesdict[ls[i].value_curve_name],
													 ls[i],
													 projected_parts_dict["predicted_finish_time_array"][i],
													 projected_parts_dict["predicted_SLR"][i]
													 )
		job_projected_values = numpy.array([projected_value_function(i) for i in range(len(ls))])
		
		return job_projected_values

def apply_function_to_tasklist(function, ls, projected_parts_dict):
	curvesdict = ls[0].parent_job.parent_application_manager.curvesdict
	
	projected_value_function = lambda id : function(
											   curvesdict[ls[id].parent_job.value_curve_name],
											   ls[id].parent_job,
											   projected_parts_dict["predicted_finish_time_array"][id],
											   projected_parts_dict["predicted_SLR"][id]
											   )
	task_projected_values = [projected_value_function(id) for id in range(len(ls))]
	
	return task_projected_values






def rank_function_by_projected_slr_tasks(ls, projected_parts_dict):
	return projected_parts_dict["predicted_SLR"]

def rank_function_by_projected_slr_jobs(ls, projected_parts_dict):
	return projected_parts_dict["predicted_SLR"]



def rank_function_by_pslr_sf_tasks(ls, projected_parts_dict):
	#finishes = projected_parts_dict["predicted_finish_time_array"]
	#tcps = projected_parts_dict["task_sub_critical_path_array"]
	jcps = projected_parts_dict["job_critical_path_array"]

	waits_acc = projected_parts_dict["waits"]

	n = numpy.max(jcps)
	
	pslr_sf = projected_parts_dict["predicted_SLR"] + (numpy.floor(waits_acc/n) ** 2)
	
	
	return pslr_sf

def rank_function_by_pslr_sf_jobs(ls, projected_parts_dict):
	return rank_function_by_pslr_sf_tasks(ls, projected_parts_dict)



def rank_function_by_projected_value_tasks(ls, projected_parts_dict):
	
	return value_calculator_vectored(ls, projected_parts_dict)
	

def rank_function_by_projected_value_jobs(ls, projected_parts_dict):
	return apply_function_to_joblist(actual_value_calculator, ls, projected_parts_dict)


def rank_function_by_value_density_jobs(ls, projected_parts_dict):

	#job_projected_values = apply_function_to_joblist(actual_value_calculator, ls, projected_parts_dict)
	job_projected_values = rank_function_by_projected_value_tasks(ls, projected_parts_dict)

	job_projected_value_densities = job_projected_values / projected_parts_dict["job_critical_path_array"]
	return job_projected_value_densities

def rank_function_by_value_density_tasks(ls, projected_parts_dict):
	
	#this is equivalent to the dynamic value density function given by
	#Dynamic Value-Density for Scheduling Real-Time Systems (1999) alarmi and burns
	#but applied to a non-pre-emptive workflow case using my upward ranks, instead of the pre-emptive case
	#in their paper
	
	task_projected_values = rank_function_by_projected_value_tasks(ls, projected_parts_dict)
	task_projected_value_densities = task_projected_values / projected_parts_dict["task_sub_critical_path_array"]
	return task_projected_value_densities


def rank_function_by_value_density_square_jobs(ls, projected_parts_dict):
	job_projected_values = apply_function_to_joblist(actual_value_calculator, ls, projected_parts_dict)
	job_projected_value_densities = job_projected_values / numpy.square(projected_parts_dict["job_critical_path_array"])
	return job_projected_value_densities

def rank_function_by_value_density_square_tasks(ls, projected_parts_dict):
	
	#this is the DVD-2 (dynamic value density squared) function from aldarmi and burns (1999)
	#print 'i think this is wrong!!!' - pretty sure this is corrected now.
	task_projected_values = rank_function_by_projected_value_tasks(ls, projected_parts_dict)
	task_projected_value_densities = task_projected_values / numpy.square(projected_parts_dict["task_sub_critical_path_array"])
	return task_projected_value_densities







def rank_function_by_projected_remaining_value_jobs(ls, projected_parts_dict):
	return apply_function_to_joblist(potential_positive_value_remaining, ls, projected_parts_dict)

def rank_function_by_projected_remaining_value_tasks(ls, projected_parts_dict):
	return apply_function_to_tasklist(potential_positive_value_remaining, ls, projected_parts_dict)










def rank_function_by_raw_value_density_jobs(ls,
											predicted_finish_time_array,
											predicted_SLR,
											job_critical_path_array,
											task_sub_critical_path_array):
	
	pass
	print "error, unimplemented yet"
	exit()
	return ls




class projected_generic_orderer(object):
	def __init__(self, kind_name, real_name, backward = False, sum_not_cp = False):
		self.name = real_name
		print "\t\tcreating a" + real_name + " orderer"
		self.workload_loaded = False
		self.debugging_on = True
		self.backward = backward
		self.sum_not_cp = sum_not_cp
	
		if kind_name == "projected_slr":
			self.ranking_task_function = rank_function_by_projected_slr_tasks
			self.ranking_job_function = rank_function_by_projected_slr_jobs
		elif kind_name == "projected_value":
			self.ranking_task_function = rank_function_by_projected_value_tasks
			self.ranking_job_function = rank_function_by_projected_value_jobs
		elif kind_name == "projected_value_density":
			self.ranking_task_function = rank_function_by_value_density_tasks
			self.ranking_job_function = rank_function_by_value_density_jobs
		elif kind_name == "projected_value_remaining":
			self.ranking_task_function = rank_function_by_projected_remaining_value_tasks
			self.ranking_job_function = rank_function_by_projected_remaining_value_jobs
		elif kind_name == "pslr_sf":
		   self.ranking_task_function = rank_function_by_pslr_sf_tasks
		   self.ranking_job_function = rank_function_by_pslr_sf_jobs
		elif kind_name == "pvd_sq":
			self.ranking_task_function = rank_function_by_value_density_square_tasks
			self.ranking_job_function = rank_function_by_value_density_square_jobs
		else:
			print "on initialising the generic projected orderer, unknown orderer kind:", kind_name


	def initialise_with_workload(self, simulation_parameters, joblist, network_manager = None):
		self.debugging_on = simulation_parameters["debugging_on"]
		
		if "networking_enabled" in simulation_parameters.keys():
			if simulation_parameters["networking_enabled"]:
				if network_manager == None:
					print "networking enabled but no network manager supplied to" + self.name + " orderer"
					exit()
				else:
					self.networking_enabled = True
			else:
				self.networking_enabled = False
		
					
		if self.name not in ("projected_slr", "pslr_sf"):
						
			if "value_enabled" in simulation_parameters.keys():
				if not simulation_parameters["value_enabled"]:
					print "error: trying to init value scheduling but value is disabled"
					exit()
			else:
				print "error: trying to initialise value scheduler without enabling value parameter key"
				exit()
		
		self.workload_loaded = True
		
		self.job_critical_path = {}
		self.job_submit = {}
		self.task_sub_critical_path = {}
		
		numtasks = sum([len(j.tasklist) for j in joblist])
		
		self.task_subs_array = numpy.ones(numtasks)
		self.job_cps_array = numpy.ones(numtasks)
		self.job_submits_array = numpy.ones(numtasks)
		
		taskcounter = 0
		self.taskid_to_index = {}
		
		job_metric_calculator = job_metrics(simulation_parameters, network_manager)
		
		for j in joblist:
			self.job_submit[j.name] = j.submit_time
			
			
			if self.networking_enabled:
				jcp, tcsps = network_manager.up_exec_time_with_network_delay(j, estimate = True, sum_instead = self.sum_not_cp)
			elif not self.sum_not_cp:
				jcp, tcsps = job_metric_calculator.up_exec_time(j, estimate = True)
			else:				
				jcp, tcsps = job_metric_calculator.up_exec_time(j, estimate = True, sum_instead = True)
				
			self.job_critical_path[j.name] = jcp #add the job critical path to the job critical path dictionary
			self.task_sub_critical_path.update(tcsps) #add the task remaining critical path to do to the task cp dicitionary
			
			for t in j.tasklist:
				
				self.task_subs_array[taskcounter] = numpy.float64(self.task_sub_critical_path[t.taskid])
				self.job_cps_array[taskcounter] = numpy.float64(self.job_critical_path[t.parent_job.name])
				self.job_submits_array[taskcounter] = numpy.float64(self.job_submit[t.parent_job.name])
				
				self.taskid_to_index[t.taskid] = taskcounter
				taskcounter += 1
		
		if self.debugging_on:
			print "projected generic orderer initialised"
			
			print len(self.task_subs_array)
			print self.task_subs_array[0:100]
			print self.job_cps_array[0:100]
			print self.job_submits_array[0:100]


	def ordered_jobs(self, ls):
		if self.debugging_on:
			if not self.workload_loaded:
				print "error: workload has not yet been loaded for the" + self.name + " orderer - jobs"
				exit()
			print "-- generic job orderer ordering this list:", [j.name for j in ls], "at", now()
		
		
		if len(ls) <= 1:
			return ls
		
		job_cps = []
		job_submits = []
		now_np = numpy.float64(now()+1) #massively interesting fudge factor - does this help? !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		
		for j in ls:
			job_cps.append(numpy.float64(self.job_critical_path[j.name]))
			job_submits.append(numpy.float64(self.job_submit[j.name]))
		
		job_critical_path_array = numpy.array(job_cps)
		job_submits_array = numpy.array(job_submits)
		
		waits = numpy.float64(now()) - job_submits_array
		job_pred_finish_time = now_np + job_critical_path_array
		predicted_SLR = (job_pred_finish_time - job_submits_array) / job_critical_path_array
				
		parts_dict = {"predicted_finish_time_array": predicted_finish_time_array,
					  "predicted_SLR": predicted_SLR,
					  "job_critical_path_array": job_critical_path_array,
					  "task_sub_critical_path_array": task_sub_critical_path_array,
					  "waits": waits}
				
		rank_values = self.ranking_job_function(ls, parts_dict)
		
		if import_manager_local.use_pypy:
			sorted_jobs = self.from_list_one(rank_values, ls)
		else:
			sorted_jobs = self.from_list_four(rank_values, ls)
	
		

		if backward:
			sorted_jobs = sorted_jobs.reverse()
			
		if self.debugging_on:
			sorted_slrs_ascending = numpy.take(rank_values, indices)
			if backward:
				sorted_slrs = sorted_slrs_ascending
			else:
				sorted_slrs = sorted_slrs_ascending[::-1]
			print "orderer job order given:", [(sorted_jobs[id].name, round(sorted_slrs[id], 5)) for id in range(len(ls))]
		
		return sorted_jobs


	def from_list_one(self, predicted_SLR, ls):
		task_and_pslrs = [(ls[i], predicted_SLR[i]) for i in range(len(ls))]
		sorted_tasks_and_pslrs = sorted(task_and_pslrs, key=lambda x: x[1], reverse = True)
		sorted_tasks = [s[0] for s in sorted_tasks_and_pslrs]
		return sorted_tasks


	def from_list_four(self, predicted_SLR, ls):
		task_and_pslrs = zip(predicted_SLR, range(len(ls)))
		task_and_pslrs.sort(reverse=True)
		
		sorted_tasks = [ls[i[1]] for i in task_and_pslrs]
		
		return sorted_tasks


	def calc_arrays_numpy_pypy(self, ls):
		task_indices = set([self.taskid_to_index[t.taskid] for t in ls])
		task_sub_critical_path_array = numpy.array([self.task_subs_array[t] for t in task_indices])
		job_critical_path_array = numpy.array([self.job_cps_array[t] for t in task_indices])
		job_submits_array = numpy.array([self.job_submits_array[t] for t in task_indices])
		
		return task_sub_critical_path_array, job_critical_path_array, job_submits_array
	
	
	def calc_arrays_numpy_real(self, ls):
		task_indices = numpy.array([self.taskid_to_index[t.taskid] for t in ls])
		task_sub_critical_path_array = self.task_subs_array[task_indices]
		job_critical_path_array = self.job_cps_array[task_indices]
		job_submits_array = self.job_submits_array[task_indices]
		
		return task_sub_critical_path_array, job_critical_path_array, job_submits_array
	
	
	def ordered(self, ls): #the core of the projected value orderer ******************
		#for a list of tasks
		
		
		if self.debugging_on:
			if not self.workload_loaded:
				print "error: workload has not yet been loaded for the projected-generic orderer - tasks"
				exit()
			print "-- projected generic task orderer ordering this list:", [t.taskid for t in ls], "at", now()
		
		if len(ls) <= 1:
			return ls
		
		
		now_np = numpy.float64(now())
		
		
		if import_manager_local.use_pypy:
			task_sub_critical_path_array, job_critical_path_array, job_submits_array =\
				self.calc_arrays_numpy_pypy(ls)
		else:
			task_sub_critical_path_array, job_critical_path_array, job_submits_array =\
				self.calc_arrays_numpy_real(ls)
		
		predicted_finish_time_array = now_np + task_sub_critical_path_array
		
					
		waits_acc = numpy.float64(now()) - job_submits_array
		predicted_SLR = ((predicted_finish_time_array + 1) - job_submits_array) / job_critical_path_array
		#predicted_SLR = ne.evaluate('((predicted_finish_time_array + 1) - job_submits_array) / job_critical_path_array')
		#pslr3 = ((numpy.float64(now() + 1) + task_sub_critical_path_array) - job_submits_array) / job_critical_path_array
		
		#NB: at this stage, p-slr can be < 1.0 for tasks not on the critical path
					
		parts_dict = {"predicted_finish_time_array": predicted_finish_time_array,
			"predicted_SLR": predicted_SLR,
			"job_critical_path_array": job_critical_path_array,
			"task_sub_critical_path_array": task_sub_critical_path_array,
			"waits": waits_acc}
			
		rank_values = self.ranking_task_function(ls, parts_dict)

		if self.debugging_on:
			print "projected generic metrics for tasks:"
			print [(ls[i].taskid,rank_values[i]) for i in range(len(ls))]
		
		
		#for tasks not on the critical path (and on a dep chain tail that doesn't link back to the CP) - the predicted SLR could be below 1
		#this isn't a problem. those will only get run if the tasks that are on the CP have been run first. as time passes with not being
		#run, they'll rise up to having an SLR above 1 anyway
		
		if import_manager_local.use_pypy:
			sorted_tasks = self.from_list_one(rank_values, ls)
		else:
			sorted_tasks = self.from_list_four(rank_values, ls)
		
		if self.backward:
			sorted_tasks.reverse()

		if self.debugging_on:
			#sorted_slrs_ascending = numpy.take(task_projected_values, indices)
			sorted_values = sorted(rank_values)
			if self.backward:
				sorted_values.reverse()
			
			#print "value task order given:", [(sorted_tasks[id].taskid, round(sorted_values[id], 5)) for id in range(len(ls))]

			print "vt2:", [(t.taskid) for t in sorted_tasks]


		return sorted_tasks

























"""





class projected_value_orderer(object):
	def __init__(self):
		self.name = "projected_value"
		print "creating a projected value orderer"
		self.workload_loaded = False
		self.debugging_on = True
		self.backward = False
	
	
	
	def initialise_with_workload(self, simulation_parameters, joblist, network_manager = None):
		self.debugging_on = simulation_parameters["debugging_on"]
		
		if "networking_enabled" in simulation_parameters.keys():
			if simulation_parameters["networking_enabled"]:
				if network_manager == None:
					print "networking enabled but no network manager supplied to projected_value orderer"
					exit()
		
		if "value_enabled" in simulation_parameters.keys():
			if not simulation_parameters["value_enabled"]:
				print "error: trying to init value scheduling but value is disabled"
				exit()
		else:
			print "error: trying to initialise value scheduler without enabling value parameter key"
			exit()
		
		#self.load_value_curves_file(simulation_parameters)
		
		self.workload_loaded = True
		
		self.job_critical_path = {}
		self.job_submit = {}
		self.task_sub_critical_path = {}
		
		
		numtasks = sum([len(j.tasklist) for j in joblist])
		
		self.task_subs_array = numpy.ones(numtasks)
		self.job_cps_array = numpy.ones(numtasks)
		self.job_submits_array = numpy.ones(numtasks)
		
		taskcounter = 0
		self.taskid_to_index = {}
		
		job_metric_calculator = job_metrics(simulation_parameters, network_manager)
		
		for j in joblist:
			self.job_submit[j.name] = j.submit_time
			jcp, tcsps = job_metric_calculator.up_exec_time(j, estimate = True)
			self.job_critical_path[j.name] = jcp #add the job critical path to the job critical path dictionary
			self.task_sub_critical_path.update(tcsps) #add the task remaining critical path to do to the task cp dicitionary
			
			for t in j.tasklist:
				
				self.task_subs_array[taskcounter] = numpy.float64(self.task_sub_critical_path[t.taskid])
				self.job_cps_array[taskcounter] = numpy.float64(self.job_critical_path[t.parent_job.name])
				self.job_submits_array[taskcounter] = numpy.float64(self.job_submit[t.parent_job.name])
				
				self.taskid_to_index[t.taskid] = taskcounter
				taskcounter += 1
		
		if self.debugging_on:
			print "projected_value orderer initialised"
			
			print len(self.task_subs_array)
			print self.task_subs_array[0:100]
			print self.job_cps_array[0:100]
			print self.job_submits_array[0:100]
	
	
	def ordered_jobs(self, ls):
		if self.debugging_on:
			if not self.workload_loaded:
				print "error: workload has not yet been loaded for the value_based orderer - jobs"
				exit()
			print "-- SLR job orderer ordering this list:", [j.name for j in ls], "at", now()
		
		
		if len(ls) <= 1:
			return ls
		
		job_cps = []
		job_submits = []
		now_np = numpy.float64(now()+1) #massively interesting fudge factor - does this help? !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		
		for j in ls:
			job_cps.append(numpy.float64(self.job_critical_path[j.name]))
			job_submits.append(numpy.float64(self.job_submit[j.name]))
		
		job_critical_path_array = numpy.array(job_cps)
		job_submits_array = numpy.array(job_submits)
		
		job_pred_finish_time = now_np + job_critical_path_array
		predicted_SLR = (job_pred_finish_time - job_submits_array) / job_critical_path_array
		
		indices = numpy.argsort(predicted_SLR)
		sorted_jobs_ascending = numpy.take(ls, indices)
		sorted_jobs = sorted_jobs_ascending[::-1]
		
		if self.debugging_on:
			sorted_slrs_ascending = numpy.take(predicted_SLR, indices)
			sorted_slrs = sorted_slrs_ascending[::-1]
			print "SLR job order given:", [(sorted_jobs[id].name, round(sorted_slrs[id], 5)) for id in range(len(ls))]
		
		return sorted_jobs
	
	
	def from_list_one(self, predicted_SLR, ls):
		task_and_pslrs = [(ls[i], predicted_SLR[i]) for i in range(len(ls))]
		sorted_tasks_and_pslrs = sorted(task_and_pslrs, key=lambda x: x[1], reverse = True)
		sorted_tasks = [s[0] for s in sorted_tasks_and_pslrs]
		return sorted_tasks
	
	def from_list_two(self, predicted_SLR, ls):
		task_and_pslrs = [(predicted_SLR[i], ls[i]) for i in range(len(ls))]
		sorted_tasks_and_pslrs = sorted(task_and_pslrs, reverse = True)
		sorted_tasks = [s[1] for s in sorted_tasks_and_pslrs]
		return sorted_tasks

	def from_list_three(self, predicted_SLR, ls):
		task_and_pslrs = zip(predicted_SLR, ls) #[(predicted_SLR[i], ls[i]) for i in range(len(ls))]
		task_and_pslrs.sort(reverse=True)
		
		#sorted_pslrs, sorted_tasks = zip(*task_and_pslrs) #[s[1] for s in task_and_pslrs]
		sorted_tasks = [s[1] for s in task_and_pslrs]
		return sorted_tasks


	def from_list_four(self, predicted_SLR, ls):
		task_and_pslrs = zip(predicted_SLR, range(len(ls)))
		task_and_pslrs.sort(reverse=True)
		
		sorted_tasks = [ls[i[1]] for i in task_and_pslrs]
		
		return sorted_tasks

	
	
	
	
	
	
	
	def from_list_numpy(self, predicted_SLR, ls):
		indices = numpy.argsort(predicted_SLR)
		sorted_tasks_ascending = numpy.take(ls, indices)
		sorted_tasks = sorted_tasks_ascending[::-1]
		return sorted_tasks

	def from_list_numpy_two(self, predicted_SLR, ls):
		indices = numpy.argsort(predicted_SLR)[::-1]
		sorted_tasks_ascending = numpy.take(ls, indices)
		#sorted_tasks = sorted_tasks_ascending
		return sorted_tasks_ascending

	def calc_arrays_old(self, ls):
		task_subs = []
		job_cps = []
		job_submits = []
		
		
		for t in ls:
			task_subs.append(numpy.float64(self.task_sub_critical_path[t.taskid]))
			job_cps.append(numpy.float64(self.job_critical_path[t.parent_job.name]))
			job_submits.append(numpy.float64(self.job_submit[t.parent_job.name]))
		
		task_sub_critical_path_array = numpy.array(task_subs)
		job_critical_path_array = numpy.array(job_cps)
		job_submits_array = numpy.array(job_submits)
		
		return task_sub_critical_path_array, job_critical_path_array, job_submits_array

	def calc_arrays_numpy_pypy(self, ls):
		task_indices = set([self.taskid_to_index[t.taskid] for t in ls])
		task_sub_critical_path_array = numpy.array([self.task_subs_array[t] for t in task_indices])
		job_critical_path_array = numpy.array([self.job_cps_array[t] for t in task_indices])
		job_submits_array = numpy.array([self.job_submits_array[t] for t in task_indices])
		
		return task_sub_critical_path_array, job_critical_path_array, job_submits_array
	
	
	def calc_arrays_numpy_real(self, ls):
		task_indices = numpy.array([self.taskid_to_index[t.taskid] for t in ls])
		task_sub_critical_path_array = self.task_subs_array[task_indices]
		job_critical_path_array = self.job_cps_array[task_indices]
		job_submits_array = self.job_submits_array[task_indices]
		
		return task_sub_critical_path_array, job_critical_path_array, job_submits_array
	
	
	def ordered(self, ls): #the core of the projected value orderer ******************
		#for a list of tasks
		
		
		if self.debugging_on:
			if not self.workload_loaded:
				print "error: workload has not yet been loaded for the projected-value orderer - tasks"
				exit()
			print "-- projected value task orderer ordering this list:", [t.taskid for t in ls], "at", now()
		
		if len(ls) <= 1:
			return ls
		
		
		now_np = numpy.float64(now())


		if import_manager_local.use_pypy:
			task_sub_critical_path_array, job_critical_path_array, job_submits_array =\
				self.calc_arrays_numpy_pypy(ls)
		else:
			task_sub_critical_path_array, job_critical_path_array, job_submits_array =\
				self.calc_arrays_numpy_real(ls)

		predicted_finish_time_array = now_np + task_sub_critical_path_array
					
		predicted_SLR = (predicted_finish_time_array - job_submits_array) / job_critical_path_array


		task_projected_values = numpy.array(
								[actual_value_calculator(
														 ls[id].parent_job.parent_application_manager.curvesdict[ls[id].parent_job.value_curve_name],
														 ls[id].parent_job,
														 predicted_finish_time_array[id],
														 predicted_SLR[id])
								 for id in range(len(ls))])
		if self.debugging_on:
			print "projected values for tasks:"
			print [(ls[i].taskid,task_projected_values[i]) for i in range(len(ls))]


		#for tasks not on the critical path (and on a dep chain tail that doesn't link back to the CP) - the predicted SLR could be below 1
		#this isn't a problem. those will only get run if the tasks that are on the CP have been run first. as time passes with not being
		#run, they'll rise up to having an SLR above 1 anyway

		if import_manager_local.use_pypy:
			sorted_tasks = self.from_list_one(task_projected_values, ls)
		else:
			sorted_tasks = self.from_list_four(task_projected_values, ls)


		if self.debugging_on:
			#sorted_slrs_ascending = numpy.take(task_projected_values, indices)
			sorted_values = sorted(task_projected_values)
			
			print "value task order given:", [(sorted_tasks[id].taskid, round(sorted_values[id], 5)) for id in range(len(ls))]
			pass

		return sorted_tasks




"""













		


if __name__ == "__main__":
	

	
	

	value_curve =  [(1.8, 1.0), (2.0, 0.9), (2.1, 0.6), (2.7, 0.2), (2.8, 0.1), (3.0, -0.5)]


	print value_curve_valid([])
	print value_curve_valid([(2.0, 0.8), (2.1, 0.85)])

	print value_curve_valid(value_curve)

	print interpolate_factor_from_curve(value_curve, 1.9)
	
	a = [factor_calculator(value_curve, x) for x in numpy.linspace(0,5,200)]
	print a
	import matplotlib.pyplot as plt

	plt.plot(a)
	plt.show()


#	print factor_calculator(value_curve, 1.2)

	


#def projected_value_calculator(value_curve, task, time):


































"""




class simplescheduler():
	def __init__(self, nodelist):
		self.nodelist = nodelist
		self.curr_index = 0
		self.call_count = 0
	
	def best_node_for_task(self, taskid):
		self.call_count += 1
		#print "##", self.curr_index, "call count ", self.call_count
		best = self.nodelist[self.curr_index]
		self.curr_index = (self.curr_index + 1) % (len(self.nodelist))
		#print best
		return best


class better_scheduler():
#thinking I will subclass these and then implement these routines to make sure that different schedulers all correspond to the same API
	
	
	def __init__(self):
		pass
		#todo
	
	def best_resource_for_task(self, task, resource_list):
		pass
		#todo
	
	def best_router_for_job(self, job, router_list):
		pass
		#todo

	def best_resources_for_tasklist(self, tasklist, resource_list):
		pass
		#todo
	
	def best_routers_for_jobs(self, jobs, router_list):
		pass
		#todo


class random_scheduler(better_scheduler):

	def __init__(self):
		print "initialising the random scheduler"
	
	def best_resource_for_task(self, task, resource_list):
	
		if len(resource_list) == 0:
			print "fatal error, no resources given"
			print "task", task
			print "resourcelist", resource_list
			print "req kind", task.requirements_kind
			exit()
		elif len(resource_list) == 1:
			return resource_list[0]
		else:
			return random.choice(resource_list)
	
	def best_router_for_job(self, job, router_list):
		if len(router_list) == 0:
			print "fatal error, no resources given"
			exit()
		elif len(router_list) == 1:
			return router_list[0]
		else:
			rid = random.randint(0, len(router_list)-1)
			return router_list[rid]

	def best_resources_for_tasklist(self, tasklist, resource_list):
		return [resource_list[random.randint(0, len(resource_list)-1)] for t in tasklist]
	
	def best_routers_for_jobs(self, jobs, router_list):
		return [resource_list[random.randint(0, len(resource_list)-1)] for j in jobs]




class random_demand_scheduler(better_scheduler):

	def __init__(self):
		print "initialising the random demand-driven scheduler"
	
	def best_resource_for_task(self, task, resource_list):
		
		if not task.dependencies_satisfied():
			#print 'returning none'
			return None
		
		
		if len(resource_list) == 0:
			print "fatal error, no resources given"
			print "task", task
			print "resourcelist", resource_list
			print "req kind", task.requirements_kind
			exit()
		elif len(resource_list) == 1:
			return resource_list[0]
		else:
		
			readytimeresourcelist = [r.how_soon_n_cores_free(1) for r in resource_list]
			
			min_ready_time = min(readytimeresourcelist)
			
			all_min_resources = [resource_list[rid] for rid in range(len(resource_list)) if readytimeresourcelist[rid] == min_ready_time]
			
			random_res_of_mins = random.choice(all_min_resources)

			#print 'returning resource ', random_res_of_mins.name
			return random_res_of_mins
	
	def best_router_for_job(self, job, router_list):
	
		print 'error: unimplemented'
		exit()
	
		if len(router_list) == 0:
			print "fatal error, no resources given"
			exit()
		elif len(router_list) == 1:
			return router_list[0]
		else:
			rid = random.randint(0, len(router_list)-1)
			return router_list[rid]

	def best_resources_for_tasklist(self, tasklist, resource_list):
		return [resource_list[random.randint(0, len(resource_list)-1)] for t in tasklist]
	
	def best_routers_for_jobs(self, jobs, router_list):
		return [resource_list[random.randint(0, len(resource_list)-1)] for j in jobs]






class fifo_task_scheduler(better_scheduler):
	def __init__(self):
		print "initialising the fifo task scheduler"
		self.task_fifo_queue = collections.deque()
		self.callcount = 0
	
	def best_resource_for_task(self, task, resource_list):
		#watch this routine for performance - it gets called very, very often!
		#print 'finding a fifo task resource for', task.taskid
		self.callcount += 1
		
		if task not in self.task_fifo_queue:
			if task.dependencies_satisfied():
				self.task_fifo_queue.append(task)
			else:
				return None
	
		if task is not self.task_fifo_queue[0]:
			return None
	
		if not task.dependencies_satisfied():
			#print 'returning none'
			return None
		if len(resource_list) == 0:
			print "fatal error, no resources given"
			print "task", task
			print "resourcelist", resource_list
			print "req kind", task.requirements_kind
			exit()
		elif len(resource_list) == 1:
			#print 'returning ', resource_list[0]
			self.task_fifo_queue.popleft()
			return resource_list[0]
		else:
			readytimeresourcelist = [r.how_soon_n_cores_free(1) for r in resource_list]
			#print readytimeresourcelist
			res_index = readytimeresourcelist.index(min(readytimeresourcelist))
			#print res_index
			#print 'returning resource ', resource_list[res_index].name
			self.task_fifo_queue.popleft()
			return resource_list[res_index]
	
	
	
	
	def best_router_for_job(self, job, router_list):
		print "fatal error: TODO unimplemented routine1"
		exit()
		if len(router_list) == 0:
			print "fatal error, no resources given"
			exit()
		elif len(router_list) == 1:
			return router_list[0]
		else:
			#TODO
			pass
			

	def best_resources_for_tasklist(self, tasklist, resource_list):
		return [self.best_resource_for_task(t,resource_list) for t in tasklist]

	
	def best_routers_for_jobs(self, jobs, router_list):
		print "fatal error: TODO unimplemented routine3"
		exit()
		#return [resource_list[random.randint(0, len(resource_list)-1)] for j in jobs]	
	

class fifo_job_scheduler(better_scheduler):
	def __init__(self):
		print "initialising the fifo job scheduler"
		self.knowntasklist = set()
		self.callcount = 0
	
	def best_resource_for_task(self, task, resource_list):
		self.callcount += 1
		

		
		if task not in self.knowntasklist:
			if task.dependencies_satisfied():
				self.knowntasklist.update(set([task]))
			else:
				return None

		#print 'known task list',  [t.taskid for t in self.knowntasklist]

		#if task.taskid is not min(t.taskid for t in self.knowntasklist):
		#	return None
	
		if not task.dependencies_satisfied():
			#print 'returning none'
			return None
		if len(resource_list) == 0:
			print "fatal error, no resources given"
			print "task", task
			print "resourcelist", resource_list
			print "req kind", task.requirements_kind
			exit()
		elif len(resource_list) == 1:
			#print 'returning ', resource_list[0]
			self.knowntasklist.difference_update(set([task]))
			return resource_list[0]
		else:
			readytimeresourcelist = [r.how_soon_could_task_start(task) for r in resource_list]
			#print readytimeresourcelist
			res_index = readytimeresourcelist.index(min(readytimeresourcelist))
			#print res_index
			#print 'returning resource ', resource_list[res_index].name
			self.knowntasklist.difference_update([task])
			return resource_list[res_index]
	
	def best_router_for_job(self, job, router_list):
		print 'error'
		pass
		#todo

	def best_resources_for_tasklist(self, tasklist, resource_list):
		return [self.best_resource_for_task(t,resource_list) for t in tasklist]
	
	def best_routers_for_jobs(self, jobs, router_list):
		pass
		#todo


class orderer():
	def __init__():
		self.assigned_priorities = set([0])
	
	def task_ordered_priority(self,task):
		return 0

#class fifo_job_orderer(orderer):#
#	def __init__():
#		self.assigned_priorities = set([0])
#		self.task_order_priority {}
#	
#	def task_ordered_priority(self,task):
#		if task.taskid in self.task_order_priority.keys():
#			return self.task_order_priority[task.taskid]
#		else

class fifo_job_orderer():
	
	#working here
#	make the priority real number jobid integer . taskid integer over 1000 or some other large num - sufficiently large to be 10 times larger than task id can ever be. 
	#then change inits to job and task creation. unique nums?
	
	def task_ordered_priority(self, task):
		return task.taskid
"""