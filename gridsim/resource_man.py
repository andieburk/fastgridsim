# Resource Management classes for gridsim

from task_state_new import TaskState
from import_manager import import_manager
import_manager_local = import_manager()
if import_manager_local.import_trace:
	from SimPy.SimulationTrace import *
else:
	from SimPy.Simulation import *
import collections
import numpy
import helpers


class ResourceKernel(object): #should look as much as possible the same as a ResourceManager, but using the bulk manager for the heavy lifting
	def __init__(self, resource_id, parent_cluster, simulation_parameters, resource_kind, requirements_manager, bulk_resource_manager):
		self.debugging_on = simulation_parameters["debugging_on"]
		self.bulk_resource_manager = bulk_resource_manager
		self.bulk_resource_manager.new_resource(resource_id, resource_kind, self)
		self.name = resource_id
		self.resource_id = resource_id
		self.parent_cluster = bulk_resource_manager.parent_cluster
		self.iteration_count = 0
		self.requirements_manager = requirements_manager
		

	def __pre_run_checks__(self, task):
		if not self.requirements_manager.requirements_suitable(task.get_requirements_kind(), self.bulk_resource_manager.resource_kind(self.resource_id)):
			print "fatal error: task assigned to invalid architecture"
			exit()
		if not self.bulk_resource_manager.resource_free(self.resource_id):
			print "fatal error: task assigned to busy resource"
			exit()
		if task.state_manager.state != TaskState.ready:
			print "fatal error: non-ready task assigned to resource", self.resource_id
			exit()

	def free(self):
		return self.bulk_resource_manager.resource_free(self.resource_id)
	
	def get_resource_kind(self):
		return self.bulk_resource_manager.resource_kind(self.resource_id)
		
	def run_task(self, task):
		self.iteration_count += 1
		
		if self.debugging_on:
			self.__pre_run_checks__(task)
			print 'task', task.taskid, 'is trying to run on resource', self.name
			print "resource kernel", self.name, "iteration", self.iteration_count
			
		self.bulk_resource_manager.run_task(task, self.resource_id)
		
	
	def current_task(self):
		return self.bulk_resource_manager.task_running_on(resource_id)


	def expected_next_free(self): #this could be optimised further if this is a bottleneck for schedulers - use the bulk manager to store a cache of exp finish time
		if self.free():
			print "warning, requesting expected_next_free when the node is already free"
			return now()
		else:
			return self.current_task().expected_finish_time()

	def go(self):
		pass
	
	def task_finished_event(self):
		self.bulk_resource_manager.task_finish_on(self.resource_id)

			
			

class ResourceManagerBulk(object):
	def __init__(self, parent_cluster, simulation_parameters):
		self.free_resource_ids_set = set([])
		self.busy_resource_ids_set = set([])
		self.resource_id_to_kind = {}
		self.busy_resource_ids_to_task = {}
		self.resource_ids_to_kernels = {}
		self.parent_cluster = parent_cluster

	def new_task(self, taskid, resource_kind):
		self.free_resource_ids_set.add(taskid)
		self.resource_id_to_kind[taskid] = resource_kind
	
	def new_resource(self, resource_id, resource_kind, resource_kernel):
		self.free_resource_ids_set.add(resource_id)
		self.resource_id_to_kind[resource_id] = resource_kind
		self.resource_ids_to_kernels[resource_id] = resource_kernel
	
	def resource_free(self, resource_id):
		if len(self.free_resource_ids_set) < len(self.busy_resource_ids_set):
			return resource_id in self.free_resource_ids_set
		else:
			return resource_id not in self.busy_resource_ids_set
	
	def resource_kind(self, resource_id):
		return self.resource_id_to_kind[resource_id]
	
	def run_task(self, task, resource_id):
		self.free_resource_ids_set.remove(resource_id)
		self.busy_resource_ids_set.add(resource_id)
		self.busy_resource_ids_to_task[resource_id] = task
	
	
	def task_finish_on(self, resource_id):
		self.busy_resource_ids_set.remove(resource_id)
		self.busy_resource_ids_to_task[resource_id] = None
		self.free_resource_ids_set.add(resource_id)
		
	def task_running_on(self, resource_id):
		return self.busy_resource_ids_to_task[resource_id]
	
	def free_resource_ids(self):
		return self.free_resource_ids_set

	def kernel_from_resid(self, id):
		return self.resource_ids_to_kernels[id]

	def free_resources(self):
		#rids = self.free_resource_ids()
		return [self.resource_ids_to_kernels[id] for id in  self.free_resource_ids_set]
	
	def get_resource_ids_to_kernels(self, rids):
		return [self.resource_ids_to_kernels[id] for id in rids]
	
	
	def number_of_free_resources(self):
		return len(self.free_resource_ids_set)
	











class ResourceManager(Process):
	
	def __init__(self, resource_id, parent_cluster, simulation_parameters, resource_kind, requirements_manager):
		Process.__init__(self, name=resource_id)
		self.actualresource = Resource(capacity=1, name=resource_id, unitName="core", preemptable=False, monitored=True)
		self.task_finished_event = SimEvent(name=resource_id+"_task_finished_event")
		self.debugging_on = simulation_parameters["debugging_on"]
		self.resource_kind = resource_kind
		self.parent_cluster = parent_cluster #TODO test this
		self.requirements_manager = requirements_manager
		self.free_bool = True
		self.current_task = None

	def __pre_run_checks__(self, task):
		if not self.requirements_manager.requirements_suitable(task.get_requirements_kind(), self.resource_kind):
			print "fatal error: task assigned to invalid architecture"
			exit()
		if not self.free_bool:
			print "fatal error: task assigned to busy resource"
			exit()
		if task.state_manager.state != TaskState.ready:
			print "fatal error: non-ready task assigned to resource"
			exit()

	def free(self):
		return self.free_bool
	
	def get_resource_kind(self):
		return self.resource_kind
		
	def run_task(self, task):
		# a few checks to ensure that tasks are not incorrectly executed
		if self.debugging_on:
			self.__pre_run_checks__(task)
			print 'task', task.taskid, 'is trying to run on resource', self.name
		
		self.free_bool = False
		self.current_task = task


	def expected_next_free(self):
		if self.free_bool:
			print "warning, requesting expected_next_free when the node is already free"
			return now()
		else:
			return self.current_task.expected_finish_time()

	def go(self):
		#this should only be triggered when the task completes execution
		iteration_count = 0
		while True:
			iteration_count += 1
			if self.debugging_on:
				print "resource manager", self.name, "iteration", iteration_count
			yield waitevent, self, self.task_finished_event
			self.current_task = None
			self.free_bool = True
			#self.parent_cluster.update.signal()
			

class Cluster(Process):
	
	def __init__(self, clusterid, simulation_parameters):
		Process.__init__(self,name=clusterid)
		self.simulation_object_type = "cluster"
		self.tasklist = set([])
		self.joblist = []
		self.orderer = simulation_parameters["cluster_orderer"]
		self.allocator = simulation_parameters["cluster_allocator"]
		self.debugging_on = simulation_parameters["debugging_on"]
		self.update = SimEvent(name=clusterid+"_update_event")
		self.iteration = 0
		self.alloc_iteration = 0
		self.resources = None
		self.last_update = -1
		self.popped_task_ids = set([])
		self.assigned_resource_ids = []
		self.subkindscached = False
		self.subkindscache = None
		self.resource_manager_bulk = ResourceManagerBulk(self, simulation_parameters)
		self.assign_count = 0
		self.prune_count = 0
		self.cluster_optimised_return_count = 0
		self.global_complete_at_time_counter = None
		
	
	def set_global_completion_counter(self, gc_counter):
		self.global_complete_at_time_counter = gc_counter
	
	def set_resources(self, resources):
		if self.debugging_on:
			self.__check_resources__(resources)
		self.resources = resources
		

	def __check_resources__(self, resources):
		#a cluster needs at least one resource
		if len(resources) < 1:
			print "error, no resources specified for a cluster"
			exit()
			
		#clusters should only be homogeneous - why? could I lift this restriction?
		resource_kinds = [r.get_resource_kind() for r in resources]
		num_kinds = len(set(resource_kinds))
		if num_kinds != 1:
			print "clusters should be homogeneous. more than one kind of resource specified"
			exit()

	def get_kinds(self):
		if not self.subkindscached:
			kindlist = [r.get_resource_kind() for r in self.resources]
			kindset = set(kindlist)
			self.subkindscache = kindset
			self.subkindscached = True
			
		return self.subkindscache
	
	def max_sub_cores(self):
		return self.get_cluster_core_count()
	
	def get_cluster_core_count(self):
		if len(self.resources) > 0:
			return len(self.resources)
		else:
			print "no resources defined when get_cluster_core_count called"
			exit()

	def assign_job(self, job):
		if self.debugging_on:
			self.assign_count += 1
			print "time", now(), "assign count", self.assign_count, "job name", job.name
		self.joblist.append(job)
		self.tasklist = self.tasklist | set(job.tasklist)
	
	def assign_tasks(self, tasks):
		self.tasklist = self.tasklist | set(tasks)
	
	def decompose_queued_jobs(self):
		for j in self.non_started_jobs:
			self.joblist.append(j)
			self.tasklist = self.tasklist | set(j.tasklist)
	"""
	def task_allocation_on_update(self):
		if self.last_update < now():
			self.popped_task_ids = []
			self.assigned_resource_ids = []
			self.last_update = now()
		
		enough_cores = True
		tasks_to_assign = True
		procs_available = True
		
		#prune completed tasks
		self.tasklist = [t for t in self.tasklist if (t.state_manager.state != TaskState.completed)] #a HUGE speed optimisation: >100x increase
		
		while tasks_to_assign and procs_available and enough_cores:
		
			self.alloc_iteration += 1
			#list of ready tasks
			readytasks = [t for t in self.tasklist 
							if ((t.state_manager.state == TaskState.ready)
							and (t.taskid not in self.popped_task_ids))]
			self.alloc_iteration += len(self.tasklist) * len(self.popped_task_ids)
			#list of ready processors
			free_resources = [r for r in self.resources 
								if (r.free()
								and (r.name not in self.assigned_resource_ids))]
			self.alloc_iteration += len(self.tasklist) * len(self.assigned_resource_ids)
				
			#loop tests
			tasks_to_assign = len(readytasks) > 0
			procs_available = len(free_resources) > 0
			
			if tasks_to_assign and procs_available:
				
				#list scheduling/ordering: order the list of tasks to schedule and return the head of the list
				ordered_tasklist = self.orderer.ordered(readytasks)
				next_task_to_schedule = ordered_tasklist[0]
				
				enough_cores = len(free_resources) >= next_task_to_schedule.corecount
				
				#print enough_cores, [r.name for r in free_resources], next_task_to_schedule.taskid
				
				if enough_cores:
					#list scheduling/allocation: have the scheduler return what it considers the 'best' resources for this many cores
					best_free_resources = self.allocator.best_for_task(next_task_to_schedule, next_task_to_schedule.corecount, free_resources)
					
					#assign the task to the resources
					for single_resource in best_free_resources:
						single_resource.run_task(next_task_to_schedule)
					
					#assign the resoures to the task
					next_task_to_schedule.assign(best_free_resources) 
					
					#loop controls - because many updates can happen on a single tick
					self.popped_task_ids.append(next_task_to_schedule.taskid)
					self.assigned_resource_ids.extend([b.name for b in best_free_resources])

	
	"""
	
	def prune_tasklist(self):
		if len(self.tasklist) == 0:
			self.prune_count = 0
			return
		else:
			self.prune_count += 1
			
			if self.prune_count > 100:
			
				c = len(self.tasklist)
				
				ts_complete = TaskState.completed
				
				self.tasklist = set([t for t in self.tasklist if (t.state_manager.state < ts_complete)]) #a HUGE speed optimisation: >100x increase. Note - implementation detail that >= is because of the definition of the enumerated task set types. if the id definition of failed or timed_out is changed - this needs to be changed.
				self.prune_count = 0
				
				if self.debugging_on:
					print "cleared", c - len(self.tasklist), "tasks from tasklist of length", len(self.tasklist)
			
	def cluster_ready_tasks(self):
		if len(self.tasklist) == 0:
			return []
		
		ts_ready = TaskState.ready
		
		readytasks1 = [t for t in self.tasklist if t.state_manager.state == ts_ready and t.taskid not in self.popped_task_ids]
		#readytasks = [t for t in readytasks1 if ]
		
		#and (t.taskid not in self.popped_task_ids))]
		#print len(readytasks), "tasks ready", (len(readytasks)/(float(len(self.tasklist))))*100.0
		return readytasks1
	
	def cluster_ready_tasks_faster_set_intersection(self): #much faster than the linear traverse of list using status updating. happens at the C level, not at the python level. this isn't a stable routine - order is randomised. not good for fifos. fifo schedulers must use another storage mechanism for achieving the fifo ordering. this has been implemented through the task_ready_order_counter in the application manager. this is because the stable set intersection below is just too slow to be practical, given how often this routine has to be called.
		if len(self.tasklist) < 1:
			return set([])
		else:
		
			for task1 in self.tasklist:
				break
			#roughly equivalent task1 = self.tasklist[0]
			all_ready_tasks = task1.parent_job.parent_application_manager.ready_tasks
						
			if len(all_ready_tasks) < 1:
				return set([])
			
			this_cluster_ready_tasks = self.set_list_inter(all_ready_tasks, self.tasklist)
			#this_cluster_ready_tasks = [t for t in self.tasklist if t in all_ready_tasks]
			
			not_popped = this_cluster_ready_tasks - self.popped_tasks
			
			#[t for t in this_cluster_ready_tasks if t.taskid not in self.popped_task_ids] #todo - make this a set operation too.
			
			return list(not_popped)
			
	def set_list_inter(self, set_a, set_b):
		if len(set_a) <= len(set_b):
			newset = set_a & set_b
		else:
			newset = set_b & set_a
		#newset = set_a & set_b #set intersection - the slowest line in the whole program
		return newset
	
	def set_inter_numpy(self, set_a, set_b):
		new_arr = numpy.intersect1d(set_a, set_b, assume_unique=True)
		return new_arr
	
	
	def cluster_ready_tasks_numpy_two(self):
		
		if len(self.tasklist) < 1:
			return []
		
		column_names = 'task,state,not_popped'

		state = TaskState.ready
		
		table_data_list = [[t, t.state_manager.state == state, (t.taskid not in self.popped_task_ids)] for t in self.tasklist]
		
		mini_db_table = numpy.rec.fromrecords(table_data_list, names=column_names)
		
		b = numpy.logical_and(mini_db_table["state"],mini_db_table["not_popped"])
		
		readytask_recs = mini_db_table[b]
		
		readytasks = [r["task"] for r in readytask_recs]
		
		return readytasks
	
	
	
	
	
	def cluster_ready_tasks_faster_set_intersection_different(self):
		if len(self.tasklist) < 1:
			return set([])
		else:
			for task1 in self.tasklist:
				break
			#roughly equivalent task1 = self.tasklist[0]
			all_ready_tasks = set([t.taskid for t in task1.parent_job.parent_application_manager.ready_tasks])

			this_cluster_ready_tasks = set([t for t in self.tasklist if t.taskid in all_ready_tasks])
			#self.set_list_inter(all_ready_tasks, self.tasklist)
			#this_cluster_ready_tasks = [t for t in self.tasklist if t in all_ready_tasks]
			
			not_popped = this_cluster_ready_tasks - self.popped_tasks
			
			#[t for t in this_cluster_ready_tasks if t.taskid not in self.popped_task_ids] #todo - make this a set operation too.
			
			return not_popped	
	
	def cluster_ready_tasks_numpy_implementation(self):
		if len(self.tasklist) < 1:
			return set([])
		else:
			for task1 in self.tasklist:
				break
			
			ready_task_ids_array = numpy.array(list(task1.parent_job.parent_application_manager.ready_tasks))
			this_cluster_tasklist_array = numpy.array(list(self.tasklist))
			
			#print type(ready_task_ids_array), ready_task_ids_array, "\n\n\n\n"
			#print type(this_cluster_tasklist_array), this_cluster_tasklist_array
			
			this_cluster_ready = self.set_inter_numpy(ready_task_ids_array, this_cluster_tasklist_array)
			
			not_popped = set(this_cluster_ready) - self.popped_tasks
			
			#this_cluster_ready_not_popped = set([t for t in this_cluster_ready if t not in t])
			
			return not_popped
		

	def cluster_ready_tasks_faster_set_intersection_stable(self): #much faster than the linear traverse of list using status updating. happens at the C level, not at the python level.
		#this is now deprecated. keeping tasklist as a set all the time. no ordering information stored in the tasklist.
		#all orderers must perform useful scheduling. otherwise ordering will be random.
		if len(self.tasklist) < 1:
			return set([])
		else:
			all_ready_tasks = self.tasklist[0].parent_job.parent_application_manager.ready_tasks
			this_cluster_ready_tasks = all_ready_tasks & set(self.tasklist) #set intersection
			
			rt_list = [t for t in this_cluster_ready_tasks if t.taskid not in self.popped_task_ids]
			rt_list_sorted_by_original_order = sorted(rt_list, key = lambda x: self.tasklist.index(x))
			
			return rt_list_sorted_by_original_order

			

	def cluster_ready_tasks_faster_dict_lookup(self): #set intersection is much faster - new versions of python perform set intersection using the dictionary hashing
		if len(self.tasklist) < 1:
			return set([])
		else:
			all_ready_tasks = self.tasklist[0].parent_job.parent_application_manager.ready_tasks
			this_cluster_tasklist_dict = dict(((t, 0) for t in self.tasklist))
			
			this_cluster_ready_tasks = [t for t in all_ready_tasks if t in this_cluster_tasklist_dict]
			return set(this_cluster_ready_tasks)

	
	def ordered_tasklist_calc(self, readytasks):
		return collections.deque(self.orderer.ordered(readytasks))
	


	def task_allocation_on_update_faster(self):
		#this routine is complicated because it is heavily optimised. it is called a huge number of times throughout the execution of a simulation. any inefficiency here has a huge impact.
		#only update things if all the tasks that are going to have finish on this time tick have finished. otherwise race conditions that aren't properly handled by the underlying SimPy library (unfortunately). This has been confirmed by the developer of the library, and they aren't going to be fixed - their non-determinism is a 'feature'. 
		if not self.global_complete_at_time_counter.all_tasks_finished_going_to_finish_at_now():
			if self.debugging_on:
				print ":::: not updating because not all tasks that are going to finish have yet"
			return
		
		if self.last_update < now():
			self.popped_tasks = set([])
			#self.assigned_resource_ids = []
			self.last_update = now()
			self.prune_tasklist()

			
		
		"""
		#trying to optimise how many times the cluster updates (as that is the really slow bit)
		
		if len(self.tasklist) == 0:
			all_ready_tasks = set([])
		else:
			for task1 in self.tasklist:
				all_ready_tasks = task1.parent_job.parent_application_manager.ready_tasks	
				break

		
		new_cl_readies = all_ready_tasks
		new_cores_free = self.resource_manager_bulk.number_of_free_resources()
		
		tl_state_changes = new_cl_readies - self.last_cluster_ready_tasks
		were_state_changes = len(tl_state_changes) > 0
		were_cores_free_changes = new_cores_free != self.last_cores_free
		
		if (not were_state_changes) and (not were_cores_free_changes):
			#print "optimised return, no state changes and no cores_free changes since last update"
			self.cluster_optimised_return_count += 1
			return
		"""
		#main update				
			

		enough_cores = True
		tasks_to_assign = True
		procs_available = True
		
		init_cores_free = self.resource_manager_bulk.number_of_free_resources()
		
		if init_cores_free == 0:
			return

		readytasks = self.cluster_ready_tasks_faster_set_intersection() #schedulers be careful. this randomises the order of the list!

		tasks_to_assign = len(readytasks) > 0

		if tasks_to_assign:
			ordered_tasklist = self.ordered_tasklist_calc(readytasks)
		
		while tasks_to_assign and procs_available and enough_cores:
		
			#loop tests
			tasks_to_assign = len(readytasks) > 0
			procs_available = self.resource_manager_bulk.number_of_free_resources() > 0
			
			if tasks_to_assign and procs_available:
				
				#list scheduling/ordering: order the list of tasks to schedule and return the head of the list
				
				readytasks = ordered_tasklist
				
				next_task_to_schedule = ordered_tasklist[0]
				
				enough_cores = self.resource_manager_bulk.number_of_free_resources() >= next_task_to_schedule.corecount
				
				#print enough_cores, [r.name for r in free_resources], next_task_to_schedule.taskid
				
				if enough_cores:
					#list scheduling/allocation: have the scheduler return what it considers the 'best' resources for this many cores
					nt = ordered_tasklist.popleft()
					self.popped_tasks.add(nt)
					
					if self.debugging_on:
						print nt.taskid, "come up for scheduling at", now()
					
					best_free_resource_ids = self.allocator.best_for_task(next_task_to_schedule, next_task_to_schedule.corecount, list(self.resource_manager_bulk.free_resource_ids()))
					
					best_free_resources = self.resource_manager_bulk.get_resource_ids_to_kernels(best_free_resource_ids)
					
					#assign the task to the resources
					for single_resource in best_free_resources:
						single_resource.run_task(next_task_to_schedule)
					
					#assign the resoures to the task
					next_task_to_schedule.assign(best_free_resources)

					
					#loop controls - because many updates can happen on a single tick - this may now be fixed.
					#self.popped_task_ids.add(next_task_to_schedule.taskid)
					#self.assigned_resource_ids.extend([b.name for b in best_free_resources])
		
		"""
		for task1 in self.tasklist:
			all_ready_tasks = task1.parent_job.parent_application_manager.ready_tasks	
			break
		#roughly equivalent task1 = self.tasklist[0]
					
		if len(self.tasklist) == 0:
			all_ready_tasks = set([])
		else:
			for task1 in self.tasklist:
				all_ready_tasks = task1.parent_job.parent_application_manager.ready_tasks	
				break

		self.last_cores_free = self.resource_manager_bulk.number_of_free_resources()
		"""
			
		
	def go(self):
		if self.debugging_on and (self.resources == None):
			print self.resources
			print len(self.resources)
			print "error, no resources in activating cluster"
			exit()
		
		while True :
			self.iteration += 1
			if self.debugging_on:
				print "cluster", self.name, "iteration", self.iteration, "alloc_iteration", self.alloc_iteration, "at", now()
				#print [(t.taskid, t.state_manager.state) for t in self.tasklist]
			self.task_allocation_on_update_faster()
			yield waitevent, self, self.update



class Router(Process):
	def __init__(self, router_id, sublist, requirements_manager, simulation_parameters):
		Process.__init__(self, name=router_id)
		self.simulation_object_type = "router"
		self.update = SimEvent(name=router_id+" updating trigger")
		self.jobqueue = []
		self.tasklist = []
		self.reqs = requirements_manager
		self.iteration = 0
		self.sublist = sublist
		self.sub_kinds = self.all_sub_kinds()
		self.orderer = simulation_parameters["router_orderer"]
		self.allocator = simulation_parameters["router_allocator"]
		self.debugging_on = simulation_parameters["debugging_on"]
		self.mscorescached = False
		self.mscorescache = None
		self.subiteration = 0
		self.last_jobqueue_update = -1
		
		#this version of router only supports homogeneous jobs. notable lack!
		#TODO implement task queues and job splitting at higher levels across multiple clusters
		
	
	def get_kinds(self):
		return self.sub_kinds
	
	def assign_job(self, job):
		self.jobqueue.append(job)
	
	def assign_tasks(self, tasks_to_assign):
		self.tasklist.extend(tasks_to_assign)
	
	def all_sub_kinds(self):
		kindlist = []
		for s in self.sublist:
			kindlist.extend(s.get_kinds())
		return set(kindlist)
	
	def max_sub_cores(self):
		if not self.mscorescached:
			self.mscorescache = max([s.max_sub_cores() for s in self.sublist])
			self.mscorescached = True
		return self.mscorescache
	
	def sub_satisfactory(self, job, sub):
		requirements_satisfactory = self.reqs.set_of_requirements_satisfied(
										job.architectures_required(), sub.get_kinds())
		cores_satisfactory = sub.max_sub_cores() >= job.max_cores_required()
		return requirements_satisfactory and cores_satisfactory
		
		
	def valid_subs_for_job(self, job):
		rlist = [s for s in self.sublist if self.sub_satisfactory(job, s)]
		any_valid = len(rlist) > 0
		return any_valid, rlist
	
	
	def assign_all_taskqueue(self):
		#group all tasks by kind and by job.
		#assign each group of job/kinds together. this stops there being needless comms between processors
		#most of the time, will only be a single job being considered - so not a great problem for anti-load balancing.
		
		#print "supposed to be assigning all taskqueue"
		
		if self.debugging_on:
			print self.name, "assigning tasklist", [t.taskid for t in self.tasklist]
		
		updated_subs = []
		
		if len(self.tasklist) > 0:
			
			class virtual_job(object): #the bare essentials of a job - just enough to pretend to be a job so that the job sub-finding bits of a router can be re-used
				def __init__(self, tasklist, kind, debugging_on):
					self.tasklist = tasklist
					self.kind = kind
					self.debugging_on = debugging_on
					
				def architectures_required(self):
					return set([self.kind])
				
				def max_cores_required(self):
					return max([t.get_core_count() for t in self.tasklist])
			
			tasklist_by_job_by_kind = collections.defaultdict(lambda: [])
			
			for t in self.tasklist:
				tasklist_by_job_by_kind[(t.parent_job.name, t.get_requirements_kind())].append(t)
			
			for ta in tasklist_by_job_by_kind.items():
				
				
				kind = ta[0][1]
				tlist = ta[1]
			
				#print "allocating a tasklist", [t.taskid for t in tlist], "of kind", ta[0][1]
					
							
				vjob = virtual_job(tlist, kind, self.debugging_on)
				any_valid, possible_subs = self.valid_subs_for_job(vjob)
				
				if not any_valid:
					print "error: no valid subs for a task",[(t.taskid, t.get_requirements_kind()) for t in vjob.tasklist]
					exit()
					
							
				best_sub = self.allocator.best_for_job(vjob, possible_subs)
				#print "best sub for ", [(t.taskid, t.get_requirements_kind()) for t in tlist], "is", best_sub.name
				
				self.assign_tasks_to_sub(tlist, best_sub)
				updated_subs.append(best_sub)
			
			#for s in updated_subs:
			#		s.update.signal()
			
			self.tasklist = []
		
		return updated_subs

	def assign_job_to_sub(self, job, sub):
		sub.assign_job(job)
	
	def assign_tasks_to_sub(self, tasks_to_assign, sub):
		sub.assign_tasks(tasks_to_assign)
	
	def assign_all_jobqueue(self):
		updated_subs = []
		if len(self.jobqueue) > 0:
			if self.debugging_on:
				print "router", self.name, "assigning", len(self.jobqueue), "jobs at time", now()
			ordered_queue = self.orderer.ordered_jobs(self.jobqueue)
			
			for j in self.jobqueue:
				any_valid_subs, possible_subs = self.valid_subs_for_job(j)
				if any_valid_subs:				
					#push the whole job down the tree
					#print [s.name for s in possible_subs]
					best_sub = self.allocator.best_for_job(j, possible_subs)
					self.assign_job_to_sub(j, best_sub)
					updated_subs.append(best_sub)
				else:
					#break up the jobs into their component tasks to be assigned individually
					self.tasklist.extend(j.tasklist)

					
			#for s in updated_subs:
			#	s.update.signal()
			
			self.jobqueue = []
			
		return updated_subs

	def go(self):
		while True:
			self.iteration += 1
			if self.debugging_on:
				print "router", self.name, "iteration", self.iteration
			subs1 = self.assign_all_jobqueue()
			#print "blah 1"
			subs2 = self.assign_all_taskqueue()
			
			for s in set(subs1 + subs2):
				s.update.signal()
			
			yield waitevent, self, self.update



if __name__ == "__main__":
	pass















"""

class router_old(Process):
	def __init__(self, router_id, scheduler, level, is_resource_only, sublist, requirements_manager):
		Process.__init__(self,name=router_id)
		self.router_trigger_event = SimEvent(name=router_id+" updating trigger")
		self.taskqueue = []
		self.jobqueue = []
		self.not_ready_job_queue = []
		self.is_resource_only = is_resource_only
		self.reqs = requirements_manager
		self.router_id = router_id
		self.iteration = 0
		
		#a router will either have subrouters, or subresources, but never both.
		if self.is_resource_only :
			self.subresources = sublist
			self.subrouters = []
			self.subresources_available = [s.get_resource_kind() for s in sublist]
		else :
			self.subresources = []
			self.subrouters = sublist
			
			self.subresources_available = []
			for s in sublist:
				self.subresources_available.extend(s.get_resource_kinds())
			
		self.scheduler = scheduler
		self.level = level
	
	
	def get_resource_kinds(self):
		return set(self.subresources_available)
	
	
	def valid_subs_for_task(self, task):
		
		tk = task.get_resource_kind()
			
		if self.is_resource_only:
			
			rlist = [s for s in self.subresources if self.reqs.requirements_suitable(tk, s.get_resource_kind()) ]
			
			if len(rlist) == 0:
				print "we have a problem here!"
				exit()			
			
			return rlist
		else:
			valid = []
						
			for s in self.subrouters:
				#print s.get_resource_kinds()
				truthlist = [self.reqs.requirements_suitable(tk, k) for k in s.get_resource_kinds()]
				#print truthlist
				if listcontainstrue(truthlist):
					valid.append(s)
					
			if len(valid) == 0:
				print "we have a problem here too!"
				print self.reqs.memoising_dict
				exit()					
			
			return valid
	
	
	def allocate_all_tasks(self, number):
		
		new_task_list = [self.taskqueue[i] for i in range(len(self.taskqueue)) if not self.allocate_a_task(self.taskqueue[i])] #this actually performs the allocations by side-effect of the allocate_a_task call
		self.taskqueue = new_task_list
			
	
	def allocate_a_task(self, task):
		node = self.scheduler.best_resource_for_task(task, self.valid_subs_for_task(task))
		if node is None:
			return False
		else:
			self.single_task_next_level_down(task, node)
			return True
	
	def single_task_next_level_down(self, task, node):
		if self.is_resource_only:
			node.add_new_task(task)
		else:
			node.assign_task(task)

	
	
	def allocate_a_job(self, job):

		arch_reqs = job.architectures_required()
		possible_subrouters = [sub for sub in self.subrouters if arch_reqs.issubset(sub.get_resource_kinds())]

		if len(possible_subrouters) > 0:
		#if a job can be completely performed by a sub-router (ie, the subrouter has hardware of all the right requirements to satisfy the job)
		#then pass the whole job down to the best subrouter.
			best_router = self.scheduler.best_router_for_job(job, possible_subrouters)		
			best_router.assign_job(job)
		
		else:
		#otherwise it will be necessary to break up the job here into tasks, and then allocate the tasks to where they can have resources.
			for t in job.tasklist:
				self.taskqueue.append(t)
			job.exec_router_home = self
	
	
	def allocate_all_jobs(self, number):		
		for j in range(number):
			job = self.jobqueue.pop()
			self.allocate_a_job(job)
		
	
	def update_pending_job_list(self):
		all_jobs = []
		all_jobs.extend(self.not_ready_job_queue)
		all_jobs.extend(self.jobqueue)
		self.not_ready_job_queue = [j for j in all_jobs if j.go_time < now()]
		if len(self.not_ready_job_queue) > 0:
			soonest_job_wake_up_time = min([j.go_time for j in self.not_ready_job_queue])
			self.router_trigger_event.signal(at=soonest_job_wake_up_time)
		self.jobqueue = [j for j in all_jobs if j.go_time >= now()]
	
	
	def assign_job(self, job):
		self.jobqueue.insert(0,job)
		self.router_trigger_event.signal()
		
		
	def assign_task(self, task):
		self.taskqueue.insert(0,task)
		self.router_trigger_event.signal()
	
	def go(self):
		while True :
			#print self.router_id, 'iteration', self.iteration, 'at ', now()
			self.iteration += 1
			self.update_pending_job_list()
			self.allocate_all_jobs(len(self.jobqueue))
			self.allocate_all_tasks(number=len(self.taskqueue))
			if self.is_resource_only:
				subs_updating_this_tick = [s for s in self.subresources if s.task_finished_not_yet_updated]
				for s in subs_updating_this_tick:
					s.resource_manager_update_event.signal()
			
			
			yield waitevent, self, self.router_trigger_event






















class resource_manager_old(Process):
	#sufficiently low-level that jobs will not be considered - only tasks.
	#job scheduling and dependencies will happen in the 'routers'.
	#tasks can still require many cores, as well as having 'priorities'
	#routers will manage assignment to hardware that is satisfactory
	#a resource manager will only look after one lot of hardware
	#the hardware in a resource manager will be homogeneous and have a single storage pool
	
	
	#at the moment - each core will have a certain amount of ram uniquely allocated
	#this is a simplifying assumption - in reality, many cores share a single pool of ram.
	#have to wait and see what to do about that.
	
	
	
	

	def __init__(self, resource_id, resource_kind, requirements_manager, simulation_parameters):
		Process.__init__(self,name=resource_id)
		self.resource_manager_update_event = SimEvent(name=resource_id+" update trigger")
		self.last_assign_time = -1
		self.next_finish_time = -1
		self.resource_kind = resource_kind
		self.requirements_manager = requirements_manager
		corecount = self.requirements_manager.get_kind_values(self.resource_kind)['quantity']['cores'] #TODO improve these lookups
		self.actualresource = Resource(capacity=corecount, name=resource_id, unitName="coreunique", qType=PriorityQ, preemptable=False, monitored=True) #also TODO - make all 'quantity' types a resource with appropriate contention
		self.taskqueue = []
		self.pendingtaskset = []
		self.finishtime_at_least = 0 #a monotonically increasing value to record the time the current volume of work will be completed by.
		self.task_finished_not_yet_updated = True
		self.orderer = simulation_parameters['orderer']

	def how_soon_n_cores_free(self, num_cores):
		
		if self.actualresource.capacity > 1:
			print "TODO error: how_soon_n_cores_free routine has not yet had multicore calculations implemented"
			exit()
		
		if self.finishtime_at_least > now():
			return self.finishtime_at_least
		else:
			return now()
	
	def how_soon_could_task_start(self, task):
		#similar to how soon n cores free, but taking into account the 'priority' of the task
		#working here
		base_t = now()
		if self.next_finish_time > now():
			base_t = self.next_finish_time
			
		highers_sum = sum([t.exectime for t in self.taskqueue if t.parent_job.job_priority >= task.parent_job.job_priority])
		
		return base_t + highers_sum		
		
		
	
	def simple_task_finish_estimate(self, task):
		waiting_task_durations = [i.parent_task.exectime for i in self.actualresource.waitQ]
		total_work_waiting = sum(waiting_task_durations) + task.exectime
		return float(total_work_waiting)/float(self.cpucorecount)
		
	def better_task_finish_estimate(self,task):
		#TODO
		return 0
	
	def get_resource_values(self):
		return self.resource_manager.get_kind_values()
	
	def get_resource_kind(self):
		return self.resource_kind

	
	def new_task_update_finishtime(self, task):
		if self.finishtime_at_least <= now():
			self.finishtime_at_least = now() + task.exectime
		else:
			self.finishtime_at_least += task.exectime
	
	
	
		
	def add_new_task(self, task):
		#this check may well be redundant, but leave it in for now as it could catch serious errors
		if not self.requirements_manager.requirements_suitable(task.get_resource_kind(), self.resource_kind):
			print "fatal error: task assigned to invalid architecture"
			exit()
		
		task.set_execution_resource(self)		
		
		if task.state == TaskState.ready:
			self.taskqueue.insert(0, task) #TODO make this more efficient
			self.new_task_update_finishtime(task)
			#task.exec_resource_manager = self
			#self.schedule_queued_work()
		else:
			self.pendingtaskset.insert(0, task)
			
		self.resource_manager_update_event.signal()
		

	
	
	def schedule_queued_work(self):
		#print now(), 'taskqueue', [t.taskid for t in self.taskqueue]
		if self.last_assign_time < now() and self.next_finish_time <= now() :
			if len(self.taskqueue) > 0:
				
				#print self.name, "actually scheduling at", now()
				
				self.taskqueue, task = self.orderer(self.taskqueue)
				
				#task = self.taskqueue.pop()
				task.exec_resource_manager = self
				
				#print '### actually allocating task', task.taskid, 'to', self.actualresource.name ,'at ', now()
				
				task.assign(self.actualresource)
				self.last_assign_time = now()
				self.next_finish_time = now()+task.exectime
				#tm = now() + task.exectime
				#print 'should next tick at', tm
				#self.resource_manager_update_event.signal(at=tm)
			
		
		#for t_id in range(len(self.taskqueue)):
		#	task = self.taskqueue.pop()
		#	print '### actually allocating task', task.taskid, 'to', self.actualresource.name ,'at ', now()
		#	task.assign(self.actualresource)

	
	def update_pending_list(self):
		newpending = [t for t in self.pendingtaskset if t.state is not TaskState.ready]
		newready   = [t for t in self.pendingtaskset if t.state is     TaskState.ready]
		
		self.pendingtaskset = newpending
		for t in newready:
			self.taskqueue.insert(0, t)
			self.new_task_update_finishtime(t)

	
	def update_task_in_pending_list(self, task):
		self.tasksqueue.put(self.pendingtaskset.pop(self.pendingtaskset.index(task)))
	
	def get_trigger_event(self):
		return self.resource_manager_update_event
	
	def go(self):
		it = 0
		while True:
			#print 'resource ', self.name, 'iteration ', it, 'at', now(), 'pendinglist', [t.taskid for t in self.pendingtaskset], 'active list', [t.taskid for t in self.taskqueue]
			it += 1
			self.update_pending_list()
			self.schedule_queued_work()
			task_finished_not_yet_updated = False
			yield waitevent, self, self.resource_manager_update_event
	
			




	#def get_data_from(self): # this is so that network latencies can be modelled
	
"""
