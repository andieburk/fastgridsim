#application manager

#library imports
import collections
import os.path
import cPickle as pickle
import random
import math
import datetime
import copy


#other simulation imports


from import_manager import import_manager
import helpers
from network_delay_manager import network_delay_manager, state_man_with_networking
from task_state_new import TaskState

from value_calculations import read_curves_file
from simulation_exceptions import simerr
from inter_arrivals import jsc2


import_manager_local = import_manager()
if import_manager_local.import_trace:
	from SimPy.SimulationTrace import *
else:
	from SimPy.Simulation import *
	
if import_manager_local.use_pypy:
	import numpypy as numpy
else:
	import numpy





"""
class task_microkernel_new(Process):
	def __init__(self, name, exectime, parent_task):
		Process.__init__(self, name=name)
		self.exectime = exectime
		self.parent_task = parent_task
		self.start_event = SimEvent(name=name+" task microkernel start")

	def start_on_node(self, noderef):
		self.noderef = noderef
		#self.actualresource = noderef.actualresource
		self.start_event.signal()
	
	def go(self):
#		print "task microkernel", self.name, "activating at", now()
		yield waitevent, self, self.start_event
#		print "task microkernel", self.name, "starting at", now()
		self.parent_task.set_exec_params_on_start(self.noderef)
		#yield request, self, self.actualresource
#		print "task microkernel", self.name, "got resource at", now()
		yield hold, self, self.exectime
#		print "task microkernel", self.name, "held until", now()
		#yield release, self, self.actualresource
#		print "task microkernel", self.name, "finished at", now()				
		self.parent_task.set_exec_params_on_finish(self.noderef)

		self.noderef.task_finished_event()		
#		print "task microkernel", self.name, "finished at", now()		
"""		


class multicore_task_microkernel(Process):
	def __init__(self, name, exectime, parent_task):
		Process.__init__(self, name=name)
		self.exectime = exectime
		self.parent_task = parent_task
		self.start_event = SimEvent(name=name+" task microkernel start")

	def start_on_nodes(self, noderefs):
		self.noderefs = noderefs
		#self.actualresource = noderef.actualresource
		self.start_event.signal()
	
	def go(self):
		yield waitevent, self, self.start_event

		self.parent_task.set_exec_params_on_start(self.noderefs)

		yield hold, self, self.exectime
				
		self.parent_task.set_exec_params_on_finish(self.noderefs)
	
		for n in self.noderefs:
			n.task_finished_event()
		













class task(object):
	
	class dep_man(object):
		def __init__(self, dependencies, dependents):
			self.dependencies = dependencies
			self.dependents = dependents
			self.dependency_names = [t.taskid for t in self.dependencies]
			self.dependency_finished_array = [False for t in self.dependencies]
			self.dependencies_satisfied_cached = False
			
		def dependency_completed(self, task):
			ind = self.dependency_names.index(task.taskid)
			self.dependency_finished_array[ind] = True
		
		def dependencies_satisfied(self):
			if self.dependencies_satisfied_cached:
				return True
			else:	
				result = all(self.dependency_finished_array)
				self.dependencies_satisfied_cached = result
				return result
	
	class state_man(object):
		def __init__(self, parent_task):
			self.parent_task = parent_task
			self.ready_order_id = None
			if parent_task.dependency_manager.dependencies == []:
				self.state = TaskState.ready
				self.parent_task.parent_job.parent_application_manager.ready_tasks.add(self.parent_task)
				self.parent_task.ready_order_id = self.parent_task.parent_job.parent_application_manager.task_ready_order_counter.newid()
			else:
				self.state = TaskState.stalled
				self.parent_task.parent_job.parent_application_manager.pending_tasks.add(self.parent_task)
				
				
		
		def when_all_dependencies_completed(self):
			#if editing this - check the alternate implementation in the network delay manager
			if self.state == TaskState.stalled:
				self.parent_task.parent_job.parent_application_manager.pending_tasks.remove(self.parent_task)
				self.parent_task.parent_job.parent_application_manager.ready_tasks.add(self.parent_task)
				self.parent_task.ready_order_id = self.parent_task.parent_job.parent_application_manager.task_ready_order_counter.newid()


			elif (self.state != TaskState.timed_out) and (self.state != 4):
				print "error, trying to transition to ready from ", self.state
				exit()
			#else: if state == timed out
				#pass #don't do anything
				
		
	
		def when_task_parent_job_times_out(self):
			
			if self.state == TaskState.stalled:
				self.state = TaskState.timed_out
				self.parent_task.parent_job.parent_application_manager.pending_tasks.remove(self.parent_task)
			elif self.state == TaskState.ready:
				self.state = TaskState.timed_out
				self.parent_task.parent_job.parent_application_manager.ready_tasks.remove(self.parent_task)
			else:
				#state is already completed, or failed, so don't need to do anything
				pass

		
		def when_task_starts_running(self):
			if self.state == TaskState.ready:
				self.state = TaskState.running
				
				self.parent_task.parent_job.parent_application_manager.ready_tasks.remove(self.parent_task)
				self.parent_task.parent_job.parent_application_manager.running_tasks.add(self.parent_task)
				
			else:
				print "error, trying to transition to running from ", self.state
				exit()
		
		def when_task_completed(self):
			if self.state == TaskState.running:
				self.state = TaskState.completed
				
				self.parent_task.parent_job.parent_application_manager.running_tasks.remove(self.parent_task)
				self.parent_task.parent_job.parent_application_manager.completed_tasks.add(self.parent_task)
				
			else:
				print "error, trying to transition to completed from ", self.state
				exit()

	def __init__(self,taskid, exectime, parent_job, requirements_kind, corecount, simulation_parameters):
		self.taskid = taskid
		self.parent_job = parent_job
		self.exectime = exectime
		self.requirements_kind = requirements_kind
		self.debugging_on = simulation_parameters["debugging_on"]
		self.corecount = corecount
		
		self.startcount = 0
		self.startkerneltime = -1
		self.finishkerneltime = -1
		self.finishcount = 0
		self.completed = False
		self.starttime = None
		self.finishtime = None
		self.execnodes = None
		
		self.global_complete_at_time_counter = None
		
		self.imprecise_estimate = None
		
		
		#self.kernels = [task_microkernel_new(taskid+"_kernel_"+str(i), exectime, self) for i in range(corecount)]
		
		self.kernel = multicore_task_microkernel(taskid+"_kernel", exectime, self)
		
		
		self.activate_kernels([self.kernel])
		self.dependency_manager = None
		self.state_manager = None
		self.dependencies_set = False
		self.assigned = False
	
		self.imprecise_estimate = self.parent_job.parent_application_manager.exec_time_imprecise_manager.imprecise_estimate(self.exectime)
	
		self.imp_cores_fast = [[-1,-1],[-1,-1]]
	
	
		self.imp_cores_fast[False][False] = self.exectime
		self.imp_cores_fast[False][True] = self.exectime * self.corecount
		self.imp_cores_fast[True][False] = self.imprecise_estimate
		self.imp_cores_fast[True][True] = self.imprecise_estimate * self.corecount
	
	
	
	
	def set_activate_networking(self, networking_manager = None, simulation_parameters = None):
		if simulation_parameters != None:
			if simulation_parameters["networking_enabled"]:
				self.state_manager = state_man_with_networking("state_man_for_"+self.taskid, self, networking_manager, simulation_parameters)
				activate(self.state_manager, self.state_manager.go(), at=now())				
				if self.debugging_on:
					print "state manager is enabled with networking"
			else:
				self.state_manager = self.state_man(self)
				if self.debugging_on:
					print "state manager normal enabled"
		else:
			self.state_manager = self.state_man(self)
			if self.debugging_on:
				print "state manager normal enabled"
	
		
	def activate_kernels(self, kernels):
		for k in kernels:
			activate(k, k.go(), at=now())

			
	def set_dependencies(self, dependency_list, dependents_list):
		#print "setting deps/dependents for", self.taskid, [t.taskid for t in dependency_list], [t.taskid for t in dependents_list]
		self.dependency_manager = self.dep_man(dependency_list, dependents_list)
		self.dependencies_set = True
	
	def set_global_completion_counter(self, gc_counter):
		self.global_complete_at_time_counter = gc_counter

	def expected_finish_time(self):
		if self.state_manager.state == TaskState.running:
			return self.starttime + self.exectime
		else:
			print "error, asking for expected finish time for a task that is not running"
			exit()

	def timeout_kick(self):
		self.state_manager.when_task_parent_job_times_out()



	def set_exec_params_on_start(self, nodeid):
		

		#print "task", self.taskid, "starting on", nodeid.name
#		if self.startkerneltime == -1:
#			self.startkerneltime = now()
#		elif self.debugging_on and self.startkerneltime != now():
#			print "problem with lockstep parallelism. one kernel started later than another"
#			print self.startkerneltime, now(), nodeid.name
#			exit()
			
			
#		self.startcount += 1
#		if self.startcount == self.corecount:
		self.starttime = now()
		self.state_manager.when_task_starts_running()
		self.global_complete_at_time_counter.task_just_started(now() + self.exectime)

	def set_exec_params_on_finish(self, nodeids):	
		#print "task", self.taskid, "finished"# on", nodeid.name
#		if self.finishkerneltime == -1:
#			self.finishkerneltime = now()
#		elif self.debugging_on and self.finishkerneltime != now():
#			print "problem with lockstep parallelism. one kernel finished later than another"
#			print self.finishkerneltime, now(), nodeid.name
#			exit()		
		
#		self.finishcount +=	1
#		if self.finishcount == self.corecount:
		self.finishtime = now()
		self.state_manager.when_task_completed()
		self.completed = True
		for d in self.dependency_manager.dependents:
			d.dependency_completed(self)
			
		self.global_complete_at_time_counter.task_just_finished()
		self.parent_job.task_finish_event.signal()
		
		cluster_to_update_on_finish = self.get_exec_cluster(nodeids)
		#cluster_to_update_on_finish.update.signal()
		
		#todo fix this nasty inefficient hack
		all_clusters = self.parent_job.parent_application_manager.platform_manager.clusters
		for c in all_clusters:
			c.update.signal()
			

	def dependency_completed(self, task):
		self.dependency_manager.dependency_completed(task)
		if self.dependency_manager.dependencies_satisfied():
			if self.debugging_on:
				print "dependencies completed for", self.taskid
			self.state_manager.when_all_dependencies_completed()
	
	def dependencies_satisfied(self):
		return self.dependency_manager.dependencies_satisfied()

	def assign(self, noderefs):
		#self.activate_kernels(self.kernels)
		
		if self.debugging_on:
			print "task", self.taskid, "starting at", now(), "on", [n.name for n in noderefs]
			self.__assignment_checks(noderefs)
		self.execnodes = noderefs
		self.kernel.start_on_nodes(noderefs)
		
		#for kid in range(len(self.kernels)):
		#	self.kernels[kid].start_on_node(noderefs[kid])
		self.assigned = True
			

	def __assignment_checks(self, noderefs):
		if self.assigned:
			print "Fatal Error, task has already been assigned but assign has been called again"
			exit()
		if len(noderefs) != self.corecount:
			print "error, task asssigned to wrong number of cores", self.taskid, self.corecount, len(noderefs)
			exit()
		if self.parent_job.activation_time > now():
			print "error, trying to activate task before parent job has even been submitted!"
			exit()
	
	# ---- task getters
	
	def get_exec_cluster(self, nodes_to_consider = None):
		if self.state_manager.state == TaskState.completed or self.state_manager.state == TaskState.running:
			if nodes_to_consider == None:
				return self.execnodes[0].parent_cluster 
			else:
				return nodes_to_consider[0].parent_cluster
		else:
			raise simerr("error: requesting task get exec cluster before task has started/completed executing")
	
	def get_requirements_kind(self):
		return self.requirements_kind
	
	def get_core_count(self):
		return self.corecount
			
	def get_dependencies(self):
		return self.dependency_manager.dependencies
		
	def get_dependents(self):
		return self.dependency_manager.dependents
	
	def get_completed(self):
		return self.completed
	
	def get_execution_time_fast(self, use_imprecise, multiply_by_cores):
		return self.imp_cores_fast[use_imprecise][multiply_by_cores]
	
	def get_execution_time(self, imprecise_estimate = False):
		if not imprecise_estimate:
			return self.exectime
		elif self.imprecise_estimate != None:
			return self.imprecise_estimate
		else:
			return self.get_imprecise_exec_time()
	
	def get_imprecise_exec_time(self):
		
		if self.imprecise_estimate == None:
			self.imprecise_estimate = self.parent_job.parent_application_manager.exec_time_imprecise_manager.imprecise_estimate(self.exectime)
		
		return self.imprecise_estimate

		


class job(Process):
	def __init__(self,name, parent_application_manager, simulation_parameters):
		Process.__init__(self, name=name)
		
		if parent_application_manager == None:
			print 'error: no parent app manager given'
			exit()
		
		self.parent_application_manager = parent_application_manager
		self.debugging_on = simulation_parameters["debugging_on"]
		self.finish_counter = simulation_parameters["job_finish_counter"]
		
		self.jobid = name
		self.debugging_on = False

		self.submit_set = False
		self.submit_time = None

		self.completion_time = None
		self.activation_time = None
		self.submit_node = None
		self.fairshare_path = None
		self.completed = False
		self.tasklist = None
		self.task_requirement_kinds = None
		self.maxcores = -1
		
		self.task_finish_event = SimEvent(name=name+" task just finished")
		self.job_timeout_event = SimEvent(name=name+" job just timed out")
		self.value_curve_set = False
		self.value_curve_name = ""
		self.timeout_time = -1
		self.timed_out = False
		self.timed_out_at_end = False
		self.sum_of_task_exectimes_cores_cached_value = None
	
	
	#def post_load_inits(self, simulation_parameters, parent_application_manager):


	
	
	def set_tasklist(self, tasklist):
		self.tasklist = tasklist
		self.task_requirement_kinds = set([t.get_requirements_kind() for t in self.tasklist])
	
	def set_submission_time(self, submit_time, simulation_parameters):
		if self.debugging_on:
			print "setting job ", self.name, "submit time", submit_time
			print 
		if not self.submit_set:
			self.submit_time = submit_time
			self.submit_set = True
		elif self.debugging_on and simulation_parameters['use_supplied_submit_times']:
			print "error: setting job exec time again?"
			exit()

	def set_value_curve_name(self, value_curve_name):
		if self.debugging_on:
			print 'setting value curve name', value_curve_name, 'for job', self.jobid
			if self.value_curve_set:
				print "warning, value curve has already been set once for job:", self.jobid
		self.value_curve_name = value_curve_name
		self.value_curve_set = True


	def set_fairshare_path(self, newpath):
		self.fairshare_path = newpath
				
	def __preflight_checks__(self):
		if self.tasklist == []:
			print "fatal error: job is being set to go without having a task list defined!!"
			exit()
		if (self.submit_time is None) or (self.submit_time < 0):
			print "error: job is being set to go without having a submission time defined"
			exit()
		if self.submit_time > now():
			print "error: job being activated before submission time"
			exit()

	def at_timeout_time(self, at_end = False):
		
		#force a state update (you might not need this - possibly remove for performance improve?)
		self.task_finish_event.signal()
		
		#this is not quite the correct behaviour:
		#need to check whether there are any still pending tasks - otherwise the job will still complete
		#anyway because the already running tasks are bound to complete if they've started
		
		
		if not self.completed:
			if self.debugging_on:
				print 'job', self.name, 'timed out at', now()
			self.completed = True
			self.timed_out = True
			for t in self.tasklist:
				t.timeout_kick()
			if at_end:
				self.timed_out_at_end = True

		self.task_finish_event.signal() #to get the job to drop out of its main loop

	def go(self):
		if self.debugging_on:
			self.__preflight_checks__()
			#print "job", self.name, "starting at", now()
		#don't really wake up until it is time
		self.activation_time = now()
	

				
		if now() < self.submit_time:
			#print "submitted at ", now()
			yield hold, self, self.submit_time - now()
			#print "activated at ", now()
		
		while not self.completed:
			self.completed = all(numpy.array([t.get_completed() for t in self.tasklist]))
			yield waitevent, self, self.task_finish_event

		self.completion_time = now()
		
		self.finish_counter.tick()
		
		
	
	#getters 
	def completion_time(self):
		if self.completed:
			return self.completion_time
		else:
			print "warning: asking for completion time before completion"
			return -1
		
	def architectures_required(self):
		return self.task_requirement_kinds
	
	def max_cores_required(self):
		if self.maxcores < 0:
			self.maxcores = max([t.get_core_count() for t in self.tasklist])
		return self.maxcores
	
	def sum_of_task_exectimes(self, cores = True):
		if cores:
			if self.sum_of_task_exectimes_cores_cached_value == None:
				self.sum_of_task_exectimes_cores_cached_value = sum([t.get_execution_time() * t.get_core_count() for t in self.tasklist])
			return self.sum_of_task_exectimes_cores_cached_value
		else:
			return sum([t.get_execution_time() for t in self.tasklist])
		
	def get_submit_time(self):
		if self.submit_time is not None:
			return self.submit_time
		else:
			print "requesting submit time before it has been specified"
			exit()

	#printing for debugging
	def print_job(self):
		print "\n\n --- \n\n"
		print 'name: ', self.jobid
		for t in self.tasklist:
			print 'task ', t.taskid
			deplist = [tk.taskid for tk in t.dependency_manager.dependencies]
			print 'depends on ', deplist, 'and runs for ', t.exectime, 'started at', t.starttime, 'comp at', t.finishtime
		print "\n\n"
		print self.timed_out
		print self.timed_out_at_end




class exec_time_imprecise_manager(object):
	def __init__(self, simulation_parameters):
		self.simulation_parameters = simulation_parameters
		calc_method = self.simulation_parameters["exec_time_estimate_kind"]
		self.calc_value = float(self.simulation_parameters["exec_time_estimate_value"])
		
		print "\t\tinitialising imprecise execution time estimate manager"
		
		if calc_method == "normal_jitter":
			self.calc_method = self.normal_jitter_calc
		elif calc_method == "log_round":
			self.calc_method = self.log_round_calc
		elif calc_method == "round":
			self.calc_method = self.round_calc
		elif calc_method == "systematic_add":
			self.calc_method = self.systematic_add_calc
		elif calc_method == "systematic_multiply":
			self.calc_method = self.systematic_add_calc
		elif calc_method == "exact":
			self.calc_method = self.exact_calc
		else:
			print "error: invalid task execution time imprecision estimator supplied"
			exit()


	def imprecise_estimate(self, exectime):
	
		output_value = self.calc_method(self.calc_value, exectime)
		
		#if output value isn't an int, give error (as using discrete time steps!)
		
		if int(output_value) != output_value or not isinstance(output_value, ( int, long ) ):
			print "imprecise estimator", self.calc_method, "not returning integer value", output_value
			exit()
			
		#if output value is less than 1, try again. if still, give error, as can't have execution time less than 1
		if output_value < 1:
			output_value = self.calc_method(self.calc_value, exectime)
			if output_value < 1:
				print "imprecise estimator returning value less than 1 for execution time", output_value
				exit()
				
		return output_value
	
	
	def exact_calc(self, value, exectime):
		return exectime
	
	def normal_jitter_calc(self, value, exectime):
		#return a value sampled from a normal distribution with mean exectime and standard deviation value
		absolute_diff = (float(value) / 100.0) * float(exectime)
		output_val = random.gauss(exectime, absolute_diff)
		output_int = int(math.ceil(output_val))
		if output_int < 1:
			output_int = 1
		return output_int
	
	def log_round_calc(self, value, exectime):
		#return the next higher that is a integer power of base value.
		#effectively divides the values into categories by the value of their logs
		#useful where there is a power law distribution of exec times - you might know
		#which class the jobs fall into, but not their exact value
		
		if value == 0:
			print "warning: value given to log_round_calc is 0"
			return 1
		
		if exectime == 0:
			print "warning: exectime given to log_round_calc is 0, cannot be possible!"
			return 1
		
		thelog = math.log(exectime, value)
		ceil_log = int(math.ceil(thelog))
		ceil_log_value = int(value) ** ceil_log
		return ceil_log_value
	
	def round_calc(self, value, exectime):
		return ((int(exectime) / int(value)) + 1) * int(value)

	def systematic_add_calc(self, value, exectime):
		return int(exectime + value)
	
	def systematic_multiply_calc(self, value, exectime):
		return int(exectime * value)







class application_manager(Process):
	def __init__(self, app_man_id, platform_manager, simulation_parameters, networking_manager = None):
		Process.__init__(self,name=app_man_id)
		self.debugging_on = simulation_parameters["debugging_on"]
		self.platform_manager = platform_manager
		self.sim_params = simulation_parameters
		
		self.timeout_manager = None
		
		self.exec_time_imprecise_manager = exec_time_imprecise_manager(self.sim_params)
		
		
		self.global_task_completion_counter = task_finish_at_time_counter()
		
		self.task_ready_order_counter = helpers.idcounter()
		self.job_ready_order_counter = helpers.idcounter()
		
		self.pending_tasks = set([])
		self.ready_tasks = set([])
		self.running_tasks = set([])
		self.completed_tasks = set([])
		
		self.sim_halt = None
		
		for c in self.platform_manager.clusters:
			c.set_global_completion_counter(self.global_task_completion_counter)
		
		self.job_finish_counter = helpers.progress_manager("\t\tsimulating... jobs completed so far:", 250)
		
		simulation_parameters["job_finish_counter"] = self.job_finish_counter
		
		print "\t\tread start"
		workload_reader = workload_loader(simulation_parameters, self)
		self.joblist = workload_reader.read_workload_from_file(self, networking_manager)
		print "\t\t", len(self.joblist), "jobs loaded"
		print "\t\tread end"
		
		#TODO task and platform kind checking here task_kinds platform_kinds =
		
		self.set_job_start_times(self.joblist, simulation_parameters)
		
		
		for j in self.joblist:
			for t in j.tasklist:
				t.set_global_completion_counter(self.global_task_completion_counter)
		
		self.load_value_curves_file(simulation_parameters)
		#print "app man completed"

	
	def set_timeout_manager(self, timeout_manager_handle):
		self.timeout_manager = timeout_manager_handle
	
	

	def load_value_curves_file(self, simulation_parameters):
		if (("value_enabled" in simulation_parameters.keys())
			and simulation_parameters["value_enabled"]):
			if "curves_filename" in simulation_parameters.keys():
				self.curvesdict = read_curves_file(simulation_parameters["curves_filename"])
			else:
				print "error: curves filename not in simulation parameters"
				print simulation_parameters
				exit()


	def get_joblist(self):
		return self.joblist
	
	def __start_times_checks__(self, sim_params):
		if len(self.joblist) < 1:
			print "trying to set job start times, but no jobs in joblist. maybe file hasn't been read yet"
			exit()
		
		if sim_params["staggered_release"]:
			if "target_utilisation" not in sim_params.keys():
				print "target_utilisation_percent not specified yet staggered release requested"
				exit()
			elif sim_params["target_utilisation"] < 1:
				print "target_utilisation_percent set too low to be useful", sim_params["target_utilisation"]
				exit()
			elif sim_params["target_utilisation"] > 999:
				print "target utilisation percent may be too high to be useful."
				print "to release jobs simultaneously, specify staggered_release to False"
				exit()
	

			
	
	def earliest_job_start_time(self):
		return min([j.submit_time for j in self.joblist])
	
	

	def set_job_start_times(self, joblist, sim_params):
		if self.debugging_on:
			self.__start_times_checks__(sim_params)
		
		use_supplied = sim_params['use_supplied_submit_times']
		
		staggered_release = sim_params['staggered_release']
		utilisation = sim_params['target_utilisation']
		num_procs = self.platform_manager.get_platform_core_count()
				
		if use_supplied and joblist[0].submit_set:
			
			self.sim_halt = joblist[-1].submit_time
			return
		
				
		if not staggered_release:
			for j in joblist:
				j.set_submission_time(0, sim_params)
	
		else:
			js_calc = jsc2(num_procs, utilisation, day_distribution_file = sim_params["day_distribution_file"], week_distribution_file= sim_params["week_distribution_file"])
			
			new_start_time = 0

			if "arrival_pattern" in sim_params.keys():
				if sim_params["arrival_pattern"] == "day_week_pattern":
					#print 'ping'
					curr_time = 0
					
					#jobtimes = [0]
					#for j in joblist:
					#	nt = js_calc.next_activation_time_with_day_week_cycles(j, curr_time)
					#	curr_time = nt
						
					#	jobtimes.append(nt)
					
					
					jobtimes = [0]
					for j in joblist:
						jobtimes.append(js_calc.next_activation_time_with_day_week_cycles(j, jobtimes[-1]))
				elif sim_params["arrival_pattern"] == "static_pattern":
					jobtimes = [0]
					for j in joblist:
						jobtimes.append(js_calc.next_activation_time_constant_utilisation(j, jobtimes[-1]))
			
			else:
				jobtimes = [0]
				for j in joblist:
					jobtimes.append(js_calc.next_activation_time_constant_utilisation(j, jobtimes[-1]))





			for i in range(len(jobtimes)-1):
				if jobtimes[i+1] < jobtimes[i]:
					print 'job times not in order error', jobtimes[i+1], jobtimes[i], i
					exit()



			"""
			import datetime
	
			new_xs = [js_calc.base_time + datetime.timedelta(minutes = n) for n in jobtimes]





			th = numpy.array([(n.weekday() * 24 ) + n.hour for n in new_xs])

			x = numpy.bincount(th[:-1])#, weights = [j.sum_of_task_exectimes() for j in joblist])
			import matplotlib.pyplot as plt

			plt.bar(range(len(x)), x )
			plt.show()
				
					
			#print jobtimes
			#exit()
			"""
			
			
			workload_volume = sum([j.sum_of_task_exectimes() for j in joblist])
			final_time = jobtimes[-1]

			total_possible_time = num_procs * final_time
			
			used_fraction = float(workload_volume) / total_possible_time
			
			self.sim_params["actual_utilisation_during_submissions"] = used_fraction * 100
			self.sim_params["nominal_days_execution"] = jobtimes[-1] / float(60*24)
						
			for jid in range(len(joblist)):
				joblist[jid].set_submission_time(jobtimes[jid], sim_params)
			
			
			if min(jobtimes) < 0:
				print "jobs submit time error"
				print min(jobtimes)
				print jobtimes
				exit()
			self.sim_halt = jobtimes[-1]

			
			"""
			joblist[0].set_submission_time(new_start_time, sim_params)
			
			prev_job_exec_sum = joblist[0].sum_of_task_exectimes()

			for j in joblist[1:]:
				j.set_submission_time(new_start_time, sim_params)
				new_start_time += int(
									  (float(prev_job_exec_sum)/float(num_procs))
									  /
									  (float(utilisation)/float(100))
									 )
				prev_job_exec_sum = j.sum_of_task_exectimes()
				#print prev_job_exec_sum, num_procs, utilisation, new_start_time
			"""	


	def next_workload_from_file(self, sim_params):
		self.workload_file_handler = open(sim_params["workload_filename"], 'r')
		self.workload_line = 0

	
	
	"""
	def go(self):
		last_job_in_list_submit_time = -1
		curr_next_submit_time = 0
		to_submit_on_next_tick = []
		
		job_submit_counter = helpers.progress_manager("job submitting", 1000)
		
		#print "application manager commence execution"
		for j in sorted(self.joblist, key=lambda j: j.submit_time):
			if last_job_in_list_submit_time == -1:
				last_job_in_list_submit_time = j.get_submit_time()
		
			if last_job_in_list_submit_time < j.get_submit_time():
				if self.debugging_on:
					print "submit waiting until", last_job_in_list_submit_time - now()

				yield hold, self, last_job_in_list_submit_time - now()
		
				#print now()
				if self.debugging_on:
					print "submitting", [s.name for s in to_submit_on_next_tick], "at", now()
				for subjob in to_submit_on_next_tick:
					job_submit_counter.tick()
					activate(subjob, subjob.go())
					subjob.job_ready_id = self.job_ready_order_counter.newid()
					self.platform_manager.toprouter.assign_job(subjob)
				self.platform_manager.toprouter.update.signal()
				#print "submitted"
				
				to_submit_on_next_tick = []
				last_job_in_list_submit_time = j.get_submit_time()
			
			to_submit_on_next_tick.append(j)
		yield hold, self, last_job_in_list_submit_time - now()
		for subjob in to_submit_on_next_tick:
			job_submit_counter.tick()
			activate(subjob, subjob.go())
			subjob.job_ready_id = self.job_ready_order_counter.newid()
			self.platform_manager.toprouter.assign_job(subjob)
		self.platform_manager.toprouter.update.signal()	
		
	"""
	def go(self):
		job_submit_counter = helpers.progress_manager("job submitting", 1000)
		jobs_by_submit = collections.defaultdict(lambda : [])

		for j in self.joblist:
			jobs_by_submit[j.get_submit_time()].append(j)

		job_submit_groups = sorted(jobs_by_submit.items(), key = lambda x : x[0])

		for jg in job_submit_groups:
			yield hold, self, jg[0] - now()
			jobs_to_submit_this_tick = jg[1]
			
			if self.debugging_on:
				print "submitting", [s.name for s in jobs_to_submit_this_tick], "at", now()

			for subjob in jobs_to_submit_this_tick:
				job_submit_counter.tick()
				activate(subjob, subjob.go())
				subjob.job_ready_id = self.job_ready_order_counter.newid()
				self.platform_manager.toprouter.assign_job(subjob)
			self.platform_manager.toprouter.update.signal()

		#timeout the simulation when everything has been submitted (to avoid long run-outs)

		if self.sim_params["staggered_release"]:
					
			holdtime = self.sim_halt - now()
					
			if holdtime < 0:
				print "error, no time for simulation"
				print "holdtime is ", holdtime
				print "sim halt time is", self.sim_halt
				exit()
			elif holdtime == 0:
				holdtime = 1
			

			yield hold, self, holdtime
					
			print "input workload consumed at ", now()
	
			self.sim_params["workload_consumed_at_days"] = now() / (60.0 * 24.0)
					
			print now() / (60.0 * 24.0), "nominal days after start"
					
					
			if self.sim_params["timeouts_enabled"] and (self.sim_halt > 0):
				
				self.timeout_manager.simulation_finished()
				print "simulation halting"
				
				end_timeout_counter = 0
				for j in [j for j in self.joblist if (not j.completed) and (not j.timed_out)]:
					#print "timing out job", j.name
					j.at_timeout_time(at_end = True)
					end_timeout_counter += 1
				self.sim_params["timed_out_at_end"] = end_timeout_counter

			else:
				self.sim_params["timed_out_at_end"] = len([j for j in self.joblist if (not j.completed) and (not j.timed_out)])
			



class task_finish_at_time_counter(object):
	def __init__(self):
		self.tf_counter = collections.defaultdict(lambda : 0)
	
	def task_just_started(self, taskfinish):
		self.tf_counter[taskfinish] += 1
	
	def task_just_finished(self):
		self.tf_counter[now()] -= 1
	
	def all_tasks_finished_going_to_finish_at_now(self):
		all_finished = self.tf_counter[now()] == 0
		if all_finished:
			del self.tf_counter[now()]
		return all_finished








class WorkloadLoadException(Exception):
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return repr(self.value)




def make_tasklist(task_string_list, parent_job, sim_params, network_manager = None):
	#sanity check
	if len(task_string_list) < 1:
		raise WorkloadLoadException("making a tasklist with an empty input list", task_string_list)

	#inits
	new_tasklist = []
	new_dependencies = []
	
	for nt in task_string_list:
		
		#set the task kind
		
		if nt[4] == None:
			kind = self.sim_params["task_unspecified_kind"]
		else:
			kind = nt[4]
		
		#create the task
		newtask = task(nt[0], int(nt[1]), parent_job, kind, int(nt[2]), sim_params)
		new_tasklist.append(newtask)
		new_dependencies.append(nt[3])
	
	#stitch dependencies (precessors and successors) together
	for source_task_id in range(len(new_tasklist)):
		dependencies = []
		dependents   = []
		for destination_task_id in range(len(new_tasklist)):
			if destination_task_id != source_task_id:
				if new_tasklist[destination_task_id].taskid in new_dependencies[source_task_id]:
					dependencies.append(new_tasklist[destination_task_id])
				if new_tasklist[source_task_id].taskid in new_dependencies[destination_task_id]:
					dependents.append(new_tasklist[destination_task_id])
		
		new_tasklist[source_task_id].set_dependencies(dependencies, dependents)#, network_manager, sim_params)
	
	#sanity check
	if len(task_string_list) != len(new_tasklist):
		raise WorkloadLoadException("tasklist and input list length don't match")
	
	return new_tasklist



def task_line_to_task_group(ln, sim_params):
	tname = ln[1]
	tcores = ln[2]
	texec = ln[3]
	tkind = ln[4].rstrip()
	
	#this is the nastiest networking hack ever. TODO fix this properly
	#print "nastily hacking in different task kinds"
	if "proportion_of_kind2" in sim_params.keys():
		if random.random() < sim_params["proportion_of_kind2"]:
			tkind = "Kind2"
	
	if len(ln) > 5:
		tdepsstr = ln[5].rstrip()
		if tdepsstr == "":
			newtaskdepids = []
		else:
			newtaskdepids = tdepsstr.split(',')
	else:
		newtaskdepids = []
	
	return [tname, texec, tcores, newtaskdepids, tkind]


def job_line_to_new_job(ln, sim_params, parent_app_man):
	
	if len(ln) >= 2:
		jobname = ln[1].rstrip()
	else:
		raise WorkloadLoadException(("no job name or submit time found in ", line))
	
	#need to add this in later in the global application runner
	#	if jobcount > self.sim_params["max_job_count"]:
	#		break
	
	curr_job = job(jobname, parent_app_man, sim_params)
	
	if len(ln) >= 3:
		submit_time = int(ln[2].rstrip())
		
		if int(submit_time) >= 0:
			curr_job.set_submission_time(submit_time, sim_params)
	
	if len(ln) >= 4:
		fairshare_path = ln[3].rstrip()
		curr_job.set_fairshare_path(fairshare_path)
	else:
		fairshare_path = ""
	
	if len(ln) >=5:
		value_curve_name = ln[4].rstrip()
		curr_job.set_value_curve_name(value_curve_name)
	
	return curr_job





class workload_loader(object):
	
	def __init__(self, sim_params, parent_app_man):
		self.sim_params = sim_params
		self.debugging_on = sim_params["debugging_on"]
		self.parent_app_man = parent_app_man
	
	def __file_read_checks__(self, sim_params):
		if "workload_filename" not in sim_params.keys():
			raise WorkloadLoadException("workload filename not specified")
		
		if sim_params["workload_filename"] == "":
			raise WorkloadLoadException("input filename blank")
		
		if not os.path.isfile(sim_params["workload_filename"]):
			raise WorkloadLoadException("workload filename specified cannot be found")
	
	def __read_in_workload_checks__(self, joblist):
		if joblist == []:
			raise WorkloadLoadException("joblist read is empty")
		for j in joblist:
			if len(j.tasklist) < 1:
				raise WorkloadLoadException(("at least one job has no tasks in the read file", invalidatasklistlens))
	
	"""
	#it looks like cached/pickled reads aren't going to work because jobs are SimPy objects local to a given SimPy instance, yet this is new every time.	
	"""
	def read_workload_from_file(self, parent_application_manager, network_manager = None):
		
		#cachepath = self.cached_file_path()
		
		#if os.path.exists(cachepath):
			#joblist = self.read_workload_from_file_pickled(cachepath)
		#else:
		joblist =  self.read_workload_from_file_direct( parent_application_manager, network_manager)
		#	print joblist[0]
		#	if not os.path.exists(cachepath): #extra check is necessary because loading can take a while and there might be a lot of threads looking at the cache folder.
		#		pickfile = open(cachepath, 'w')
		#		Pickle.dump(joblist, pickfile)
		#		pickfile.close()

		for j in joblist:
			#j.post_load_inits(self.sim_params, parent_application_manager)
			for t in j.tasklist:
				t.set_activate_networking(network_manager, self.sim_params)

		return joblist

	"""
	def cached_file_path(self):
		workload_file_path = self.sim_params["workload_filename"]
		pathmain, extension = os.path.splitext(workload_file_path)
		
		cache_suffix = "_pickcached"
		cache_extension = ".pik"
		cfp = pathmain + cache_suffix + cache_extension
		return cfp
	
	
	def read_workload_from_file_pickled(self, filepath):
		pickfile = open(filepath, 'r')
		joblist = Pickle.load(pickfile)
		pickfile.close()
		return joblist
	"""	
	
	def read_workload_from_file_direct(self,  parent_application_manager, network_manager = None,):
		#if self.debugging_on:
		self.__file_read_checks__(self.sim_params)
		
		joblist = []
		jobnames = []
		taskgroups = []
		curr_task_group = []
		tasklist = []
		
		job_loading_counter = helpers.progress_manager("Loading jobs... have so far loaded", 5000)
		
		jobcount = 0
		
		workloadinfile = open(self.sim_params["workload_filename"], 'r')
		
		for line_full in workloadinfile:
			
			line = line_full.rstrip()
			
			if line != "":
				ln = line.split(" ")
				line_marker_string = ln[0]
				
				if line_marker_string == "Job":
					jobcount += 1
					
					if jobcount > self.sim_params["max_job_count"]:
						break
					
					if jobcount > 1:
						curr_job.set_tasklist(make_tasklist(curr_task_group, curr_job, self.sim_params, network_manager))
						joblist.append(curr_job)
						job_loading_counter.tick()
						
						curr_task_group = []
					
					curr_job = job_line_to_new_job(ln, self.sim_params, parent_application_manager)
				
				elif line_marker_string == "Task":
					curr_task_group.append(task_line_to_task_group(ln, self.sim_params))
				
				elif line_marker_string[0] == "#":
					pass
				else:
					raise WorkloadLoadException(("unknown workload keyword in line", line))
				
				
		
		
		curr_job.set_tasklist(make_tasklist(curr_task_group, curr_job, self.sim_params, network_manager))
		joblist.append(curr_job)
		job_loading_counter.tick()
		
		workloadinfile.close()
		
		self.__read_in_workload_checks__(joblist)
		
		return joblist





if __name__ == "__main__":
	#y = job_submit_time_calculator(100, 100)#(self, platform_cores, desired_utilisation)
	import matplotlib.pyplot as plt

	jc = job_submit_time_calculator(100, 100)

	class fakejob(object):
		def __init__(self, time):
			self.time = time
		def get_execution_time(self):
			return self.time

	newjoblist = [fakejob(10000) for j in range(20000)]

	newtimes = [0]
	for j in newjoblist:
		nt = jc.next_activation_time_with_day_week_cycles(j, newtimes[-1])
		newtimes.append(nt)

	print newtimes[:100]

	nt = numpy.array(newtimes)

	nt_hours = (nt / 24.0) % 1.0

	plt.hist(nt, bins = 400)
	#plt.plot(nt[1:] - nt[:-1])
	plt.show()


	#exit()

#	plt.hist(newtimes, bins = 400)
#	plt.show()




