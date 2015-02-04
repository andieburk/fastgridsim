from import_manager import import_manager

import_manager_local = import_manager()
if import_manager_local.import_trace:
	from SimPy.SimulationTrace import *
else:
	from SimPy.Simulation import *

import math
import collections

class timeout_manager(Process):
	def __init__(self, name, joblist, simulation_parameters, network_manager):
		Process.__init__(self, name=name)
		
		self.sim_finished = False
		
		self.debugging_on = simulation_parameters["debugging_on"]
		
		self.networking_enabled = simulation_parameters["networking_enabled"]
		self.network_manager = network_manager
		
		
		if "job_exec_timeout_factor" in simulation_parameters.keys():
			self.timeout_factor = simulation_parameters["job_exec_timeout_factor"]
			self.timeouts_enabled = True
		
			if "timeouts_enabled" in simulation_parameters.keys():
				self.timeouts_enabled = simulation_parameters["timeouts_enabled"]
		else:
			self.timeout_factor = -1
			self.timeouts_enabled = False
		
		if self.timeouts_enabled:

			timeouts_dict = collections.defaultdict(lambda : [])
			
			for j in joblist:
				job_timeout = self.job_timeout_calculation(j)
				j.timeout_time = job_timeout
				
				timeouts_dict[job_timeout].append(j)
			
			self.sorted_job_timeout_list = sorted(timeouts_dict.items(), key=lambda x: x[0])
	

	def simulation_finished(self):
		self.sim_finished = True
	
	
	def job_timeout_calculation(self, job):
		if not self.timeouts_enabled:
			print "error: requesting job timeout calculation but timeouts disabled"
			exit()
		
		if self.networking_enabled:
			if self.network_manager == None:
				print "error: networking enabled but no manager"
				exit()

			jcp = self.network_manager.minimum_down_critical_path_for_platform(job, estimate = False)
			new_timeout = int(math.ceil(jcp * self.timeout_factor))

		else:
			#would this work better with the critical path time? (though sum of task exec times is always >= CP time)
			new_timeout = int(math.ceil(job.sum_of_task_exectimes(cores = False) * self.timeout_factor))
				
		timeout_time = job.submit_time + new_timeout
		if self.debugging_on:
			print 'job', job.name, 'submit', job.submit_time, 'exec', job.sum_of_task_exectimes(cores = False), 'timeout_inc', new_timeout, 'should time out at ', timeout_time
				
		return timeout_time
	
	
	
	def go(self):
		curr_index = 0
		if self.timeouts_enabled:
			while (not self.sim_finished) and curr_index < len(self.sorted_job_timeout_list):
			#for curr_index in range(len(self.sorted_job_timeout_list)):
				diff_to_next_time = self.sorted_job_timeout_list[curr_index][0] - now()
				
				yield hold, self, diff_to_next_time
				
				for j in self.sorted_job_timeout_list[curr_index][1]:
					print 'timing out ', j.name
					print j.submit_time
					print j.sum_of_task_exectimes(cores=False)
					
					print now()
					
					print [(t.starttime, t.finishtime, t.state_manager.state) for t in j.tasklist]
					
				
					j.at_timeout_time()
				
				curr_index += 1
				
								
								






















