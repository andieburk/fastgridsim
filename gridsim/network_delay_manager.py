import math
from import_manager import import_manager
import_manager_local = import_manager()
if import_manager_local.import_trace:
	from SimPy.SimulationTrace import *
else:
	from SimPy.Simulation import *
from task_state_new import TaskState


class network_delay_manager(object):
	def __init__(self, simulation_parameters, platform_manager, requirements_manager):
		self.level_factor = simulation_parameters["network_delay_level_power"]
		self.debugging_on = simulation_parameters["debugging_on"]
		self.communication_to_computation_ratio = simulation_parameters["communication_to_computation_ratio"]
		self.platform_manager = platform_manager
		self.requirements_manager = requirements_manager
		self.cluster_path_dict = self.cluster_path_dict_creator(self.platform_manager.toprouter.name, self.platform_manager.toprouter.sublist)
		
		#print self.cluster_path_dict
		
		self.cluster_pair_delay_cache = {}
		self.kind_pair_delay_cache = {}
		self.up_exec_time_with_network_delay_cache = {}



	def down_exec_time_with_network_delay(self, job, estimate = False):
		task_down_exec_times = {}
		
		for t in job.tasklist:
			if len(t.get_dependencies()) < 1:
				task_down_exec_times[t.taskid] = t.get_execution_time(estimate)
			else:
				deps = t.get_dependencies()
				this_task_kind = t.get_requirements_kind()
				delays = []
				
				for d in deps:
					dkind = d.get_requirements_kind()
					min_kind_delay = self.kind_difference_minimum_network_delay(dkind, this_task_kind)
					
					
					data_volume = self.real_network_delay_from_ccr(t.get_execution_time(estimate), self.communication_to_computation_ratio)
					
					
					actual_min_delay = int(math.ceil(minimum_kind_net_delay * data_volume))
					
					#actual_min_delay = int(math.ceil(min_kind_delay * d.get_execution_time(estimate) * self.communication_to_computation_ratio))
					
					delays.append(actual_min_delay + task_down_exec_times[d.taskid])
					
				max_delay = max(delays)
				
				task_down_and_net_delay = t.get_execution_time(estimate) + max_delay
					
				task_down_exec_times[t.taskid] = task_down_and_net_delay
		
		task_delays = task_down_exec_times.values()
		
		critical_path = max(task_delays)
		
		return critical_path, task_down_exec_times
					
							
		
	def up_exec_time_with_network_delay(self, job, estimate = False, sum_instead = False, no_net = False):
	
		if (job.name, estimate, sum_instead, no_net) in self.up_exec_time_with_network_delay_cache.keys():
			v = self.up_exec_time_with_network_delay_cache[(job.name, estimate, sum_instead, no_net)]
			cp = v[0]
			tups = v[1]
			return cp, tups
		#else
		
		#assumes tasklist is topologically sorted.
		task_up_exec_times = {}
		
		for t in reversed(job.tasklist):
			
			t_dependents = t.get_dependents()
			
			if len(t_dependents) > 0:	
				
				this_task_kind = t.get_requirements_kind()					
				dep_delays = []
				
				for d in t_dependents:
					#math.ceil(delays[i] * dependency_cluster_execs[i][1] * communication_to_computation_ratio)									
					
					if not no_net:
						
						dkind = d.get_requirements_kind()
						minimum_kind_net_delay = self.kind_difference_minimum_network_delay(this_task_kind, dkind)
						#print minimum_kind_net_delay
						#print d.get_execution_time()
						#print self.communication_to_computation_ratio
						
						
						data_volume = self.real_network_delay_from_ccr(t.get_execution_time(estimate), self.communication_to_computation_ratio)
						
						
						actual_min_delay = int(math.ceil(minimum_kind_net_delay * data_volume))
						
						#print actual_min_delay
					if no_net:
						actual_min_delay = 0
					
					task_previous_and_new_min_network_delay = task_up_exec_times[d.taskid] + actual_min_delay
					
					dep_delays.append(task_previous_and_new_min_network_delay)
				
				up_max = max(dep_delays)
				
				if sum_instead:
					up_max = sum(dep_delays)
				
				task_up_exec_times[t.taskid] = t.get_execution_time(estimate) + up_max
				
			else:
				task_up_exec_times[t.taskid] = t.get_execution_time(estimate)
			
			
		critical_path = max(task_up_exec_times.values())
		
		if sum_instead:
			critical_path = sum(task_up_exec_times.values())
		
		self.up_exec_time_with_network_delay_cache[(job.name, estimate, sum_instead, no_net)] = (critical_path, task_up_exec_times)
		
		return critical_path, task_up_exec_times
		


	def clusters_for_kind(self, kindid):
		#return [k for k in self.platform_manager.clusters if kindid in set(k.get_kinds())]
		return [k for k in self.platform_manager.clusters if self.requirements_manager.set_of_requirements_satisfied(set([kindid]), set(k.get_kinds()))]
		
	def cluster_path_dict_creator(self, curr_path, curr_sublist):
	
		#print "cpdc", curr_path, [n.name for n in curr_sublist]
		cluster_path_dict_part = {}
		if len(curr_sublist) == 0:
			return {}
		
		if str(type(curr_sublist[0]))[-9:-2] == "Cluster":
			for s in curr_sublist:
				cluster_path_dict_part[s.name] = curr_path+"/"+s.name
		elif str(type(curr_sublist[0]))[-8:-2] == "Router":	
			for s in curr_sublist:
				sub_dict = self.cluster_path_dict_creator(curr_path+"/"+s.name, s.sublist)
				#print sub_dict
				for i in sub_dict.items():
					cluster_path_dict_part[i[0]] = i[1]
		else:
			print "error: invalid type", type(curr_sublist[0]), "in cluster path dict creator"
			exit()
		
		if self.debugging_on:
			print "cluster path dict:", cluster_path_dict_part
		
		return cluster_path_dict_part


	def network_delay_between_two_clusters(self, cluster1, cluster2):
		if self.debugging_on:
			if len(self.cluster_path_dict.keys()) == 0:	
				print "error: in network_delay_between_two_clusters, cluster_path_dict has not yet been intialised"
				exit()
		
		if cluster1.name == cluster2.name:
			return 0
		
		elif (cluster1.name, cluster2.name) in self.cluster_pair_delay_cache.keys():
			return self.cluster_pair_delay_cache[(cluster1.name, cluster2.name)]
		
		elif (cluster2.name, cluster1.name) in self.cluster_pair_delay_cache.keys():
			return self.cluster_pair_delay_cache[(cluster2.name, cluster1.name)]
	
		else:
			cluster1path = self.cluster_path_dict[cluster1.name]
			cluster2path = self.cluster_path_dict[cluster2.name]
			
			p1split = cluster1path.split('/')
			p2split = cluster2path.split('/')
			
			lens = [len(p1split), len(p2split)]
			
			maxdepth = max(lens)
			mindepth = min(lens)
			
			top_depth_to_equal = None
			
			for depth in range(mindepth):
				if p1split[depth] == p2split[depth]:
					top_depth_to_equal = depth + 1
					
			bottom_depth_to_equal = maxdepth - top_depth_to_equal
			
			network_delay = self.network_level_delay(bottom_depth_to_equal, self.level_factor)
			
			self.cluster_pair_delay_cache[(cluster1.name, cluster2.name)] = network_delay
			
			if self.debugging_on:
				print "\\\\\\\\newly calclulated network delay between", cluster1.name, cluster2.name, "as", network_delay
			
			return network_delay

	def network_level_delay(self, level, level_factor):
		#return int(math.ceil(level ** level_factor))
		return level ** level_factor


	def kind_difference_minimum_network_delay(self, sender_kind, receiver_kind):
		
  		if (sender_kind, receiver_kind) in self.kind_pair_delay_cache.keys():
			return self.kind_pair_delay_cache[(sender_kind, receiver_kind)]
		
		if sender_kind == receiver_kind:
			self.kind_pair_delay_cache[(sender_kind, receiver_kind)] = 0
			return 0
		
		possible_sending_clusters = set(self.clusters_for_kind(sender_kind))
		possible_receiving_clusters = set(self.clusters_for_kind(receiver_kind))
		
		if len(possible_sending_clusters.intersection(possible_receiving_clusters)) > 0:
			self.kind_pair_delay_cache[(sender_kind, receiver_kind)] = 0
			return 0
		

		network_delays = [self.network_delay_between_two_clusters(c1, c2) for c1 in possible_sending_clusters for c2 in possible_receiving_clusters]
		min_network_delay = min(network_delays)
		
		self.kind_pair_delay_cache[(sender_kind, receiver_kind)] = min_network_delay
		kind_pair_delay = min_network_delay
			
		if self.debugging_on:
			print "kind pair delay for", sender_kind, receiver_kind, "is", kind_pair_delay
		
		return kind_pair_delay
	
	def task_network_delay(self, task, communication_to_computation_ratio, estimate = False):
		#calculate task with the max delay from dependencies to task - no all dependenencies. might be a big preceeding task with a small network cost.
		#calculate the time delay due to that task's execution time (via c/c ratio)
		#calculate actual delay - max of all tasks
		
		if len(task.get_dependencies()) == 0:
			return 0
		
		dependency_cluster_execs = [(tdep.get_exec_cluster(), tdep.get_execution_time_fast(estimate, False)) for tdep in task.get_dependencies()]
		poss_curr_clusters = [c for c in self.platform_manager.clusters if task in c.tasklist] #this is horrendous. but due to the intentional separation between the application and platform - as it would be in real life, I don't know of a better way of doing this. changing this to something more efficient would require very deep changes to the structure of the code, making it much less reflective of the real restrictions on communicaiton in a real grid system. ie. tasks shouldn't have to know about the workings of their platform manager to run.
		
		if len(poss_curr_clusters) > 1:
			print "error: task is in multiple cluster tasklists. this isn't good!"
			print poss_curr_clusters
			exit()
				
		if len(poss_curr_clusters) == 0:
			print "error: task is not assigned to a cluster yet it should be: task network delay isleepis being called"
			print poss_curr_clusters
			exit()
		
		this_task_curr_cluster = poss_curr_clusters[0]
		
		delays = [self.network_delay_between_two_clusters(tdep_clust[0], this_task_curr_cluster) for tdep_clust in dependency_cluster_execs]
		
		#print 'delays', delays
		
		
		delay_actual_times = [int(math.ceil(delays[i] * self.real_network_delay_from_ccr(dependency_cluster_execs[i][1],
																						 (communication_to_computation_ratio))))
							  for i in range(len(dependency_cluster_execs))]
		
		#print 'delay actual times', delay_actual_times
		
		
		max_delay = max(delay_actual_times)
		
		#print 'max delay', max_delay
		
		return max_delay
		
		
		#dependency_network_delays = [self.kind_difference_minimum_network_delay(task1.get_requirements_kind(), tdep.get_requirements_kind())
		#							 for tdep in 

	def real_network_delay_from_ccr(self, exec_time, ccr):
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
		
		
	def minimum_down_critical_path_for_platform(self, job, estimate = False):
		#kinds = set([t.requirements_kind for t in job.tasklist])
		#if len(kinds) <= 1:
			#return usual critical path
		
		task_cp_time = {}
		
		for t in job.tasklist:
		
			tdeps = t.get_dependencies()
			
			if len(tdeps) == 0:
				task_cp_time[t.taskid] = t.get_execution_time(estimate)
			else:
				
				#print "task cp time", task_cp_time
				
				
				dependency_cp_time = [task_cp_time[dep.taskid] + self.kind_difference_minimum_network_delay(dep.get_requirements_kind(), t.get_requirements_kind()) for dep in tdeps]
				this_task_cp_time = max(dependency_cp_time) + t.get_execution_time(estimate)
				task_cp_time[t.taskid] = this_task_cp_time
		
		if self.debugging_on:
			print task_cp_time
		
		return max(task_cp_time.values())
		
		
class state_man_with_networking(Process):
	def __init__(self, name, parent_task, global_networking_delay_manager, simulation_parameters):
		Process.__init__(self, name=name)
		
		self.all_deps_completed_event = SimEvent(name=name+"_all_deps_completed_event")
		
		self.parent_task = parent_task
		self.ready_order_id = None
		
		if global_networking_delay_manager == None:
			print "error: no network manager defined"
			exit()
		
		self.networking_delay_manager = global_networking_delay_manager
		self.communication_to_computation_ratio = simulation_parameters["communication_to_computation_ratio"]
		
		
		if parent_task.dependency_manager.dependencies == []:
			self.state = TaskState.ready
			self.parent_task.parent_job.parent_application_manager.ready_tasks.add(self.parent_task)
			self.parent_task.ready_order_id = self.parent_task.parent_job.parent_application_manager.task_ready_order_counter.newid()
		else:
			self.state = TaskState.stalled
			self.parent_task.parent_job.parent_application_manager.pending_tasks.add(self.parent_task)
		
		#print "state manager with networking initialised for task", self.parent_task.taskid
			
			
	
	def when_all_dependencies_completed(self):
		#print "all deps completed for", self.parent_task.taskid, "at", now()
		if self.state == TaskState.stalled:
			
			self.all_deps_completed_event.signal()
		elif self.state == TaskState.timed_out:
			pass
		else:
			print "error, trying to transition to ready from ", self.state
			exit()


	
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
		
	def go(self):
	
		yield waitevent, self, self.all_deps_completed_event
		
		sleep_time_for_network = self.networking_delay_manager.task_network_delay(self.parent_task, self.communication_to_computation_ratio)
		
		
		if sleep_time_for_network > 0:
			#print "********* task", self.parent_task.taskid, "sleeping for ", sleep_time_for_network, "to take into account network delay"
			yield hold, self, sleep_time_for_network
		
		
		#but it could time out during the network sleep - unlike in main
		
		if self.parent_task.state_manager.state == TaskState.stalled:
		
			self.parent_task.parent_job.parent_application_manager.pending_tasks.remove(self.parent_task)
			
			self.state = TaskState.ready
			self.parent_task.parent_job.parent_application_manager.ready_tasks.add(self.parent_task)
			self.parent_task.ready_order_id = self.parent_task.parent_job.parent_application_manager.task_ready_order_counter.newid()
			
			#todo fix this awful nasty inefficient hack - this isn't even the right place for this code.
			all_clusters = self.parent_task.parent_job.parent_application_manager.platform_manager.clusters
			for c in all_clusters:
				c.update.signal()	
		
		#else:

			#it timed out during the network delay
			




if __name__ == "__main__":
	
	from app_man import TaskState, task, job, application_manager
	from req_man import requirements_manager
	from resource_man import ResourceManager, Cluster, Router
	from platform_manager import platform_manager, platform_manager_constructor
	
	from import_manager import import_manager
	import_manager_local = import_manager()
	if import_manager_local.import_trace:
		from SimPy.SimulationTrace import *
	else:
		from SimPy.Simulation import *
		
	
	
	
	
	print "test run of network_delay_manager"
	


	simulation_parameters = {}
	
	simulation_parameters["workload_filename"] = "/Users/andy/Documents/Work/gridsimxcode/GridSim/gridsim/workload6.wld"
	simulation_parameters["platform_filename"] = "/Users/andy/Documents/Work/gridsimxcode/GridSim/gridsim/platform1.plat"
	simulation_parameters["network_delay_level_power"] = 3
	simulation_parameters["cluster_orderer"] = None
	simulation_parameters["router_orderer"] = None
	simulation_parameters["cluster_allocator"] = None
	simulation_parameters["router_allocator"] = None
	simulation_parameters["debugging_on"] = True
	simulation_parameters["max_job_count"] = 100000000000
	simulation_parameters["staggered_release"] = True
	simulation_parameters["target_utilisation"] = 100
	simulation_parameters["use_supplied_submit_times"] = False
	
	
	
	print '\tinitialising platform (requirements and platform managers)'
	platform_constructor = platform_manager_constructor()
	global_req_manager, global_platform_manager = \
		platform_constructor.construct_requirements_and_platform_from_file(simulation_parameters)


	print '\tinit applications'
	global_application_manager = application_manager("global_application_manager", global_platform_manager, simulation_parameters)
	
	
	ndman = network_delay_manager(simulation_parameters, global_platform_manager, global_req_manager)
	
	for j in global_application_manager.joblist:
		print ndman.minimum_down_critical_path_for_platform(j)

	for j in global_application_manager.joblist:
		print ndman.up_exec_time_with_network_delay(j)

	for k in global_req_manager.kinds.keys():
		print k, [c.name for c in ndman.clusters_for_kind(k)]
	

