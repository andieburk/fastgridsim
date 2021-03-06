from SimPy.Simulation import *
import sys
import random
import Queue
import collections
import numpy
import pylab
#import scipy

def listalltrue(ls):
	#print list
	if len(ls) == 0:
		return True
	else:
		return reduce(lambda x, y: x and y, ls)
		
		
def listcontainstrue(ls):
	if len(ls) == 0:
		return False
	elif len(ls) == 1:
		return ls[0]
	else:
		return reduce(lambda x, y: x or y, ls)
	

def strip_duplicates_in_list_of_dicts(dictlist):
	newlist = []
	for d in dictlist:
		append = True
		for n in newlist:
			if d == n:
				append = False
		if append:
			newlist.append(d)
	return newlist
	

class idcounter:
	def __init__(self):
		self.count = 0
	
	def newid(self):
		c = self.count
		self.count += 1
		return c




		



class pjob(Process):
	def __init__(self,name,parent_scheduler,simulation_parameters,go_time=0,self_task_generation=True):
		Process.__init__(self,name=name)
		self.jobid = name
		self.parent_scheduler = parent_scheduler
		self.task_finish_event = SimEvent(name=name+" task just finished")
		self.completed = False
		#self.stalled_task_list = self.tasklist
		self.priority = 1 #TODO priorities properly - including determining the best task ordering for a job.

		if self_task_generation:
			if simulation_parameters['tasklist_generator_function'] == "random":
				self.tasklist = self.generatetasklist(simulation_parameters)
			elif simulation_parameters['tasklist_generator_function'] == "fanout":
				self.tasklist = self.generate_fan_out_job(simulation_parameters)
			else:
				print "fatal error: undefined tasklist generator function"
				exit()
			self.task_requirement_kinds = set([t.get_resource_kind() for t in self.tasklist])
			
		self.submit_time = -1
		self.exec_router_home = -1
		self.go_time = go_time
		self.job_priority = 100

	def set_taskset(self, taskset):
		self.tasklist = taskset
		self.task_requirement_kinds = set([t.get_resource_kind() for t in self.tasklist])

	def go(self):
		
		self.submit_time = now()
		
		if self.tasklist == []:
			print "fatal error: job is being set to go without having a task list defined!!"
			exit()
		
		iteration = 0
		while not self.completed:
		
			#self.stalled_task_list = [t for t in self.stalled_task_list if not t.dependencies_satisfied()] #update the list of tasks that need to be checked every time a task completes in case it was dependent
		
			self.completed = listalltrue([t.completed for t in self.tasklist])	#update whether the whole job has yet completed
			
			iteration += 1

			yield waitevent, self, self.task_finish_event
			
			self.exec_router_home.router_trigger_event.signal()
		

			#print "job ", self.name, "iteration", iteration			
						#print 'running', [t.taskid for t in self.tasklist if t.state is TaskState.running]
			#print 'stalled', [t.taskid for t in self.stalled_task_list]
			#print 'deps of stalled', [([(td.taskid, td.state, td.dependencies_satisfied()) for td in t.dependencies], t.dependencies_satisfied()) for t in self.stalled_task_list]
			#ready_assigned_and_not_activated_tasks = [t for t in self.tasklist if ((t.state == TaskState.ready) and (t.got_resource) and (not t.activated))] #assign ready tasks to processors (schedule...)
			#print 'readyassignedandnotactivated', ready_assigned_and_not_activated_tasks
			
			#for t in ready_assigned_and_not_activated_tasks:
			#	print t.taskid
			#	print t.execnode.name
			#	print '####################### woooooooooooooooooooooo'
			#	t.execnode.resource_manager_update_event.signal()
			
			
#			ready_unassigned_tasks = [t for t in readytasks if t.execnode == -1]
#			for t in ready_unassigned_tasks:
#				t.assign(self.parent_scheduler.best_node_for_task(t))
#			#print [t.completed for t in self.tasklist]
			

		
		#yield hold, self, 100.0 - now()
		
		print "----> ", self.name, "completed at ", now()
	
	def set_go_time(self, time):
		if time >= 0:
			self.go_time = time
			return True
		else:
			print "negative time given - invalid"
			return False
	

	def generatetasklist(self, sim_params):

		taskcount = random.randint(sim_params['mintasks'], sim_params['maxtasks'])
		
		tasklist = []
		
		for tnumber in range(taskcount):
			tasklist.append(self.generate_single_task(tasklist,sim_params))

		return tasklist


	def generate_single_task(self, previous_task_list, sim_params):
		
		tname = "job_"+str(self.jobid)+"_task_"+str(len(previous_task_list))
		texectime = random.randint(sim_params['taskexecmin'], sim_params['taskexecmax'])
		treq = random.choice(sim_params['taskprofiles'])
		tdependencies = [previous_task_list[n] for n in range(len(previous_task_list)) if sim_params['depprob'] > random.random()]
		
		t = task(tname, texectime, self, self.parent_scheduler, self.task_finish_event, treq, tdependencies)
		
		return t
	
	
	def generate_fan_out_job(self, sim_params):
		#needs fan out breadth ("job_fan_out_breadth", "job_dependency_levels", "fan_out_task_profile")
		
		if not ("job_fan_out_breadth" in sim_params.keys() and "job_dependency_levels" in sim_params.keys() and "fan_out_task_profile" in sim_params.keys() ):
			print "fatal error for job generation: fan out parameters not included in simulation parameters"
			exit()
		
		job_exec_multiplier = 1
		
		if ('long_jobs_present' in sim_params.keys()) and sim_params['long_jobs_present']:
			if random.randint(0,100) <= sim_params['long_jobs_percent']:
				job_exec_multiplier = 10
				
		tasklist = []
		initial_task_exec_time = job_exec_multiplier * random.randint(sim_params['taskexecmin'], sim_params['taskexecmax'])
		itreq = sim_params['fan_out_task_profile']
		tdeps = []
		taskidcounter = idcounter()
		initial_task = task("job_"+str(self.jobid)+"_task_"+str(taskidcounter.newid())+"_level_init", initial_task_exec_time, self, self.parent_scheduler, self.task_finish_event, itreq, tdeps)
		tasklist.append(initial_task)
		
		level_list = []
		
		for level in range(sim_params['job_dependency_levels']-2):
			tlen = job_exec_multiplier * random.randint(sim_params['taskexecmin'], sim_params['taskexecmax'])
			this_level_list = []
			for index in range(sim_params['job_fan_out_breadth']):
				if level == 0:
					tdep = [initial_task]
				else:
					tdep = [level_list[level-1][index]]
					
				newtask = task("job_"+str(self.jobid)+"_task_"+str(taskidcounter.newid())+"_level_"+str(level), \
					       tlen,\
					       self, self.parent_scheduler,\
					       self.task_finish_event,\
					       sim_params['fan_out_task_profile'],\
					       tdep)
					       
				this_level_list.append(newtask)
			level_list.append(this_level_list)
			tasklist.extend(this_level_list)
		
		final_task_exec = job_exec_multiplier * random.randint(sim_params['taskexecmin'], sim_params['taskexecmax'])
		ftreq = sim_params['fan_out_task_profile']
		finaldeps = level_list[-1]
				
		final_task = task("job_"+str(self.jobid)+"_task_"+str(taskidcounter.newid())+"_level_final", final_task_exec, self, self.parent_scheduler, self.task_finish_event, ftreq, finaldeps)
		
		tasklist.append(final_task)
		
		return tasklist
		
		
		
	
	def architectures_required(self):
		return self.task_requirement_kinds
		
	
	
	def print_job(self):
		print "\n\n --- \n\n"
		print 'name: ', self.jobid
		for t in self.tasklist:
			print 'task ', t.taskid
			deplist = [tk.taskid for tk in t.dependencies]
			print 'depends on ', deplist, 'and runs for ', t.exectime
		print "\n\n"
	
	def sum_of_task_exectimes(self):
		#TODO make this handle multicore properly
		return sum([t.exectime for t in self.tasklist])







class resource_manager(Process):
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



def fifo_job_task_orderer_routine(tasklist):
	tasknamesindex = [int(t.taskid) for t in tasklist]
	print 'list of candidates' , tasknamesindex
	taskminindex = tasknamesindex.index(min(tasknamesindex))
	task = tasklist.pop(taskminindex)
	print 'chosen next task', task.taskid
	return tasklist, task

def fifo_orderer(tasklist):
	task = tasklist.pop()
	return tasklist, task
	

class requirements_manager():
	def __init__(self):
		self.memoising_dict = {}
		self.unnamed_kind_counter = idcounter()
		self.kinds = {}
	
	#every set of requirements will be known as a unique 'kind'
	#working with these strings is intended to be much more efficient than whole dicts all over the place
	#when operations need to actually be performed between kinds, they should really happen inside a req_manager
	#rather than passing loads of dicts around!
	
	def new_kind(self, absolute, quantity):

		possible_new = dict([('absolute', dict(absolute)), ('quantity', dict(quantity))])
		
		#check if there is an equivalent kind already present.
		all_values = self.kinds.values()
		
		for i in range(len(all_values)):
			#if equivalent exists, then return the one already there
			if possible_new == all_values[i]:
				return self.kinds.keys[i]
		
		#otherwise generate a new one and return the new kind id
		newkindid = "kind_" + str(self.unnamed_kind_counter.newid())
		
		self.kinds[newkindid] = possible_new
		
		return newkindid
	
	
	def get_kind_values(self, kind_id):
		return self.kinds[kind_id]

	
	def requirements_suitable(self, required_kind, available_kind):
		#use the memoised value if it is present to save time!
		if (required_kind, available_kind) in self.memoising_dict:
			return self.memoising_dict[(required_kind, available_kind)]

		#are all the absolute requirements present?
		if not set(self.kinds[required_kind]['absolute'].keys()).issubset(set(self.kinds[available_kind]['absolute'].keys())):
			self.memoising_dict[(required_kind, available_kind)] = False
			return False
		
		#are all the absolute requirements of the right type?
		for k in self.kinds[required_kind]['absolute'].keys():
			if self.kinds[required_kind]['absolute'][k] != self.kinds[available_kind]['absolute'][k]:
				self.memoising_dict[(required_kind, available_kind)] = False
				return False
		
		#are all the quantity requirements present?
		if not set(self.kinds[required_kind]['quantity'].keys()).issubset(set(self.kinds[available_kind]['quantity'].keys())) :
			self.memoising_dict[(required_kind, available_kind)] = False
			return False
				
		#are all the quantity requirements sufficient?
		for k in self.kinds[required_kind]['quantity'].keys():
			if self.kinds[required_kind]['quantity'][k] > self.kinds[available_kind]['quantity'][k]:
				self.memoising_dict[(required_kind, available_kind)] = False
				return False
		
		#if all the other conditions have been satisfied, return True
		self.memoising_dict[(required_kind, available_kind)] = True
		return True


	


class router(Process):
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




class platform_manager():
	def __init__(self, requirements_manager, simulation_parameters):
		self.reqs = requirements_manager
		self.router_scheduler = simulation_parameters['scheduler']
		self.resource_id_gen = idcounter()
		self.router_id_gen = idcounter()
		self.resources = []
	
		if simulation_parameters['platform_kind_id'] == 1:
			self.init_platform_one(requirements_manager)
		elif simulation_parameters['platform_kind_id'] == 2:
			self.init_platform_two(requirements_manager, simulation_parameters)
		else:
			print 'fatal error: no platform specified'
			exit()
	
	def init_platform_one(self, requirements_manager):
		
		#set up the resources
		#define the parameters for 3 kinds of machine
		ppcs_ref =   self.reqs.new_kind([('architecture', 'ppc')],                     [('cores', 2),('proc_speed', 1), ('ram', 8),  ('disk', 200 )])
		x86s_ref =   self.reqs.new_kind([('architecture', 'x86')],                     [('cores', 4),('proc_speed', 1), ('ram', 16), ('disk', 1000)])
		gpgpu_ref =  self.reqs.new_kind([('architecture', 'x86'), ('gpgpu', 'nvidia')],[('cores', 2),('proc_speed', 1), ('ram', 4),  ('disk', 100 )])
		
		#create new resources of these kinds
		ppcs_list =   [resource_manager('res_'+str(self.resource_id_gen.newid()), ppcs_ref, requirements_manager) for i in range(8)]
		x86s_list =   [resource_manager('res_'+str(self.resource_id_gen.newid()), x86s_ref, requirements_manager) for i in range(8)]
		gpgpus_list = [resource_manager('res_'+str(self.resource_id_gen.newid()), gpgpu_ref, requirements_manager) for i in range(2)]
		

		self.resources.extend(ppcs_list)
		self.resources.extend(x86s_list)
		self.resources.extend(gpgpus_list)		
		
		
		#set up the routers

		
		#set up the routers that only look after resources
		r1 = router('router_'+str(self.router_id_gen.newid()), self.router_scheduler, 1, True, ppcs_list[0:4], requirements_manager)
		r2 = router('router_'+str(self.router_id_gen.newid()), self.router_scheduler, 1, True, ppcs_list[4:8], requirements_manager)
		r3 = router('router_'+str(self.router_id_gen.newid()), self.router_scheduler, 1, True, x86s_list[0:4], requirements_manager)
		r4 = router('router_'+str(self.router_id_gen.newid()), self.router_scheduler, 1, True, x86s_list[4:8], requirements_manager)
		r5 = router('router_'+str(self.router_id_gen.newid()), self.router_scheduler, 1, True, gpgpus_list,    requirements_manager)


		#set up a higher-level (non-resource) router
		r6 = router('router_'+str(self.router_id_gen.newid()), self.router_scheduler, 2, False, [r1, r2, r3, r4, r5], requirements_manager)
		
		self.routers = [r1, r2, r3, r4, r5, r6]
		
		self.toprouter = r6
	
	
	def init_platform_two(self, requirements_manager, sim_params):
	
		procs_ref = self.reqs.new_kind([('architecture', 'x86')], [('cores', 1),('proc_speed', 1), ('ram', 1),  ('disk', 1 )])
		procs_list = [resource_manager('res_'+str(self.resource_id_gen.newid()), procs_ref, requirements_manager, sim_params) for i in range(sim_params['platform_2_proc_count'])]
		
		self.resources.extend(procs_list)
		
		r1 = router('router_'+str(self.router_id_gen.newid()), self.router_scheduler, 1, True, procs_list, requirements_manager)
		
		self.routers = [r1]
		
		self.toprouter = r1
	
	
	
	def go(self):
		for rx in self.resources:
			activate(rx, rx.go(), at=now())
		for r in self.routers:
			activate(r, r.go(), at=now())

		
	
	def get_routers(self):
		return self.routers
	
	def get_resources(self):
		return self.resources
	



def set_up_task_profiles(reqs):
	
	task_profile_list = []
	task_profile_list.append(reqs.new_kind(	[('architecture', 'x86')],				[('cores', 1), ('ram', 1), ('disk', 1)]		))#fan_out_task_profile
	task_profile_list.append(reqs.new_kind(	[('architecture', 'x86')],				[('cores', 1), ('ram', 2), ('disk', 1)]		))#startup
	task_profile_list.append(reqs.new_kind(	[('architecture', 'ppc')],				[('cores', 2), ('ram', 2), ('disk', 4)]		))#preprocessing	
	task_profile_list.append(reqs.new_kind(	[('architecture', 'x86')], 				[('cores', 4), ('ram', 16), ('disk', 80)]	))#main job
	task_profile_list.append(reqs.new_kind(	[('architecture', 'x86'),('gpgpu', 'nvidia')],		[('cores', 1), ('ram', 4), ('disk', 4)]		))#postprocessing 1
	task_profile_list.append(reqs.new_kind(	[('architecture', 'ppc')],				[('cores', 2), ('ram', 8), ('disk', 20)]	))#postprocessing 2
	
	return task_profile_list


class application_manager(Process):
	def __init__(self, sim_params, name_id, filename=""):
		Process.__init__(self,name=name_id)
		self.jobidcounter = idcounter()
		if filename is "":
			self.joblist = [pjob( 'job_'+str(self.jobidcounter.newid()), 'null', sim_params) for i in range(sim_params['numjobs'])]
		else:
			self.joblist = self.read_workload_from_file(sim_params, filename)
		
		#print self.joblist
		
		self.job_kick_event = SimEvent(name=name_id+" job kick")
		
		jobcount = len(self.joblist)
		
		for i in range(len(self.joblist)):
			self.joblist[i].job_priority = jobcount - i
			#print self.joblist[i].jobid, self.joblist[i].job_priority
		
		#set the start times of the jobs

		num_procs = sim_params['platform_2_proc_count']
		
		if sim_params['staggered_release']:
			new_start_time = 0
			prev_job_exec_sum = self.joblist[0].sum_of_task_exectimes()
			for j in self.joblist:
				j.go_time = new_start_time
				new_start_time += int((float(prev_job_exec_sum)/float(num_procs))/(float(sim_params['target_utilisation_percent'])/float(100)))
				prev_job_exec_sum = j.sum_of_task_exectimes()

	def go(self, platform_manager):
		
		jobindex = 0
		
		while jobindex < len(self.joblist):
			platform_manager.toprouter.assign_job(self.joblist[jobindex])
			activate(self.joblist[jobindex], self.joblist[jobindex].go(), at=self.joblist[jobindex].go_time)
			jobindex += 1
			if jobindex < len(self.joblist):
				yield hold, self, self.joblist[jobindex].go_time - now()
	
	
	def read_workload_from_file(self, sim_params, filename):
		
		joblist = []
		
		jobnames = []
		taskgroups = []
		curr_task_group = []
		
		jobcount = 0
		
		workloadinfile = open(filename, 'r')
		
		#lineiter = 0
		
		for line in workloadinfile:
			#lineiter += 1
			#print lineiter
			if line != "":
				ln = line.split(" ")
				if ln[0] == "Job":
					jobcount += 1
					#print jobcount
					if jobcount > 1:
						#print "assigning job"
						curr_job.set_taskset(curr_task_group)
						curr_task_group = []
						joblist.append(curr_job)
					
					curr_job = pjob(ln[1],-1,sim_params,go_time=0,self_task_generation=False)
					
					
				elif ln[0] == "Task":
					
					tdepsstr = ln[3].rstrip()
					if tdepsstr == "":
						newtasksdeps = []
					else:
						newtaskdepids = tdepsstr.split(',')
						newtasksdeps = [t for t in curr_task_group if t.taskid in newtaskdepids]
						
						#print "newtaskdepids", newtaskdepids
						#print "newtasksdeps", newtasksdeps
						
						if len(newtaskdepids) != len(newtasksdeps):
							print "not all dependencies have matched up"
							exit()
					#print "assigning task"
					curr_task_group.append(task(ln[1], int(ln[2]), curr_job, -1, curr_job.task_finish_event, all_task_profiles[0], newtasksdeps))
		
		curr_job.set_taskset(curr_task_group)
		joblist.append(curr_job)
		workloadinfile.close()
		#print joblist
		if joblist == []:
			print "error: joblist empty"
			exit()
		return joblist
		



class metrics():
	def __init__(self, joblist, proclist):
		self.joblist = joblist
		self.proclist = proclist

	
	def job_critical_path_length(self, job):
		return self.down_exec_time(job)
	
	def down_exec_time(self, job): #aka critical path length
		cumulative_times = {}
		for t in job.tasklist:
			
			if len(t.dependencies) > 0:
				cumu_sum_times = max([cumulative_times[t2] for t2 in t.dependencies])
			else:
				cumu_sum_times = 0
			cumulative_times[t] = t.exectime + cumu_sum_times
		
		return max(cumulative_times.values())
	
	
	def up_exec_time(self, job):
		joblistback = [t for t in job.tasklist]
		joblistback.reverse()
		for t in joblistback:
			self_deps = [dep_task for dep_task in job.tasklist if t in set(dep_task.dependencies)]
			deps_cumu = sum([dpt.up_cumulative_execution_time for dpt in self_deps])
			up_cumu = t.exectime + deps_cumu
			t.up_cumulative_execution_time = up_cumu

	
	
	def job_in_flight_time(self, job):
		tasklist = job.tasklist
		
		starttime = min([t.starttime for t in tasklist])
		endtime = max([t.finishtime for t in tasklist])
		
		exectime = endtime - starttime
		
		#print job.jobid , 'ran for', exectime
		
		return exectime
	
	
	def job_submit_to_finish_time(self, job):
		submit_time = job.submit_time
		endtime = max([t.finishtime for t in job.tasklist])
		return endtime - submit_time
	
	def job_sum_of_task_exec_times(self, job):
		etimesum = 0
		for t in j.tasklist:
			etimesum += t.exectime
		return etimesum

	def job_turnaround_over_critical_path_ratio(self, job):
		return float(self.job_submit_to_finish_time(job))/float(self.job_critical_path_length(job))

		
	def cluster_total_utilisation(self):
		etimesum = sum([self.job_sum_of_task_exec_times(j) for j in self.joblist])
		return 100.0 * (float(etimesum)/(len(global_platform_manager.resources) * (now()+1)))
	
	
	def utilisation_other(self):
		used_cycles = sum([sum([t.exectime for t in j.tasklist]) for j in self.joblist])
		print 'utilisation', 100.0 * (float(used_cycles) / (float(len(global_platform_manager.resources)) * float(now()+1)))
	
	
	def print_useful_metrics(self, job):
		print "\n----\n\n"
		print job.jobid
		print "was submitted at ", job.submit_time
		print "started executing at", min([t.starttime for t in job.tasklist])
		print "finished executing at", max([t.finishtime for t in job.tasklist])
		print "therefore was in flight for", self.job_in_flight_time(job)
		print "and had a turnaround time of", self.job_submit_to_finish_time(job)
		print "its critical path length is", self.job_critical_path_length(job)
		print "giving a ratio of turnaround time/critical path length of", self.job_turnaround_over_critical_path_ratio(job)
	
	def print_global_metrics(self, joblist):
		
		all_turnaround_ratios = [self.job_turnaround_over_critical_path_ratio(j) for j in joblist]
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
		

		res1 = [float(self.job_submit_to_finish_time(j))/float(self.job_critical_path_length(j)) for j in self.joblist]
		res2 = [float(self.job_in_flight_time(j))/float(self.job_critical_path_length(j)) for j in self.joblist]
		#print res1
		#print res2
		#print numpy.average(res1)
		#print numpy.average(res2)
		
		res1a = numpy.array(res1)
		res2a = numpy.array(res2)
		
		res = [	utilisation_value,\
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
		
		
		
		
		#numpy.average(res2)',', numpy.median(res1), ',', numpy.median(res2)
		#print '#-# , ',utilisation_value, ',', numpy.median(res1), ',', numpy.median(res2)
		
		
		workloadsum = sum([self.job_sum_of_task_exec_times(j) for j in self.joblist])
		print 'sum of all exec times' , workloadsum
		
		
		
		#working here



	#def job_network_costs(self, job, network_manager): TODO network costs


def generate_perfect_workload(num_procs, num_jobs, tasks_per_job, maxtime):
	#this generates a perfect workload
	#for a given number of homogeneous processors
	#this will create a workload that is schedulable
	#with 100% cpu utilisation from 0 until maxtime
	total_task_count = num_jobs * tasks_per_job
	total_split_count = total_task_count - num_procs
	
	procs = range(num_procs)
	timeslots = range(maxtime)
	
	possible_splits = [(p,t) for p in procs for t in timeslots]
	
	#pick n unique splits from all possible splits, store in 'splits'
	
	splits = random.sample(possible_splits, total_split_count)
	
	psplits = []
	for p in numprocs:
		splits_for_single_proc = sort([spl[1] for spl in splits if spl[0] == p])
		psplits.append(splits_for_single_proc)
	
	
	#each split section will be a task.
	# it will need a start time, end time, exectime, a parent job, and then some valid dependencies
	
	task_sections = [(a[i], a[i+1], a[i+1]-a[i], -1, []) for i in range(len(psplits[p]-1)) for a in psplits]
	
	#for every job , pick n unique task sections from the list of possible task sections
	random.shuffle(task_sections)
	joblist = []
	for i in range(num_jobs):
		joblist[i] = [append(task_sections.pop()) for j in range(tasks_per_job)]
			
	for j in joblist:
		for t in range(j):
			possible_deps = [ti for ti in j if ti(1) <= t(0)]
			t[4] = possible_deps #TODO make this a random subset rather than all... or a choice between
	
	#then actually set up the platform and real jobs and tasks
	
	

def first_of(value):
	return value[0]


def print_executed_schedule(joblist):
	schedule = {}
	s2 = {}
	for j in joblist:
		for t in j.tasklist:
			tinfo = [t.starttime, t.taskid, t.parent_job.jobid, t.finishtime]
			if t.execnode.name in schedule.keys():
				schedule[t.execnode.name].extend([tinfo])
				s2[t.execnode.name].extend([t.parent_job.jobid] * (t.finishtime - t.starttime))
			else:
				schedule[t.execnode.name] = [tinfo]
				s2[t.execnode.name] = [t.parent_job.jobid] * (t.finishtime - t.starttime)
	
	for execnode in schedule.keys():
		sorted_list = sorted(schedule[execnode], key = first_of)
		schedule_list = []
		for t in sorted_list:
			schedule_list.extend([str(t[0]), str(t[1]), str(t[2][:-1]), str(t[3]), '\n'])
		
		schedule_string = ' '.join(schedule_list)
		print execnode + ':\n' + schedule_string
	
	
	maxtime = max([t.finishtime for t in j.tasklist for j in joblist])
	
	vis_sched = [([0] * maxtime) for n in range(len(schedule.keys()))]
	
	for j in joblist:
		for t in j.tasklist:
			vis_sched[schedule.keys().index(t.execnode.name)][t.starttime : t.finishtime] = [t.parent_job.jobid] * (t.finishtime - t.starttime)
	
	vis_sched_arr = numpy.zeros((len(vis_sched),max([len(vis_sched[i]) for i in range(len(vis_sched))])))
	
	for i in range(len(vis_sched)):
		for j in range(len(vis_sched[i])):
			vis_sched_arr[i][j] = vis_sched[i][j]

	#pylab.subplot(2,1,1)

	c = pylab.pcolor(vis_sched_arr)

	pylab.title = 'schedule'
	
	pylab.show()
	
		
		
		
	
	
	
	
	


class timerprint(Process):
	def __init__(self):
		Process.__init__(self,name='timer1')
	def go(self):
		while True:
			print 'time ', now()
			yield hold, self, 10



		
print "starting...."

initialize()

jobcounter = idcounter()
nodecounter = idcounter()

print 'init timer'
timer = timerprint()

print 'init req manager'
global_req_manager = requirements_manager()
all_task_profiles = set_up_task_profiles(global_req_manager)

print 'init sim params'
#parameters for the workload

if len(sys.argv) < 2:
	using_command_line_variables = False
else:
	using_command_line_variables = True


if using_command_line_variables:
	print sys.argv[1], sys.argv[2], sys.argv[3]
	utilisation_value = sys.argv[2]
	if sys.argv[1] == "fifo_task":
		scheduler_value = fifo_task_scheduler()
		orderer_value = fifo_orderer
	elif sys.argv[1] == "random_demand":
		scheduler_value = random_demand_scheduler()
		orderer_value = fifo_orderer
	elif sys.argv[1] == "fifo_job":
		scheduler_value = fifo_job_scheduler()
		orderer_value = fifo_job_task_orderer_routine
	else:
		print "fatal error: invalid scheduler input"
		exit()
	workload_filename = sys.argv[3]
else:
	scheduler_value = fifo_task_scheduler()
	orderer_value = fifo_orderer
	utilisation_value = 100
	workload_filename = "workload1.wld"





sim_params_list = [		  ('random_seed', 12346),\
				   ('taskexecmin', 5),\
					('taskexecmax', 10),\
					 ('mintasks', 2),\
					  ('maxtasks', 5),\
					   ('depprob', 0.3),\
					    ('numjobs', 800),\
						 ('numnodes', 1000),\
						  ('taskprofiles', all_task_profiles),\
						   ('job_fan_out_breadth', 1),\
						    ('job_dependency_levels', 3),\
							 ('fan_out_task_profile', all_task_profiles[0]),\
							  ('scheduler', scheduler_value),\
							   ('tasklist_generator_function', 'fanout'),\
							    ('simulation_max_time', 2000000),\
							    ('staggered_release', True),\
							     ('target_utilisation_percent', utilisation_value),\
								 ('platform_kind_id', 2),\
								 ('platform_2_proc_count', 2),\
								  ('long_jobs_present', True),\
								   ('long_jobs_percent', 10),\
								    ('orderer', orderer_value)\
							      ]

simulation_parameters = dict(sim_params_list)

print 'init random seed'
random.seed(simulation_parameters['random_seed'])

print 'init platform'
#create the platform
global_platform_manager = platform_manager(global_req_manager, simulation_parameters)

print 'init applications'
#create the application(s)
global_application_manager = application_manager(simulation_parameters, "global_application_manager", filename=workload_filename)


print 'timer go'
#activate(timer, timer.go(), at=now())

print 'platform go'
#let the platform processes go
global_platform_manager.go()

print 'applications go'
#let the application processes go
activate(global_application_manager, global_application_manager.go(global_platform_manager), at=0)

print 'simulation go'

#perform the simulation
simulate(until = simulation_parameters['simulation_max_time'])

print "curr time is now ", now()



for j in global_application_manager.joblist:
	for t in j.tasklist:
		if t.starttime == -1 or t.finishtime == -1:
			print "fatal error: simulation has ended but some tasks have not run"
			print t.taskid
			print [p.taskid for jp in global_application_manager.joblist for p in jp.tasklist if p.starttime < 0]
			#print [n.taskid for n in global_platform_manager.router_scheduler.task_fifo_queue]
			exit()



#calculation of metrics

print simulation_parameters

print_executed_schedule(global_application_manager.joblist)


global_metrics_manager = metrics(global_application_manager.joblist, global_platform_manager.get_resources())

for j in global_metrics_manager.joblist:
	j.print_job()
	global_metrics_manager.print_useful_metrics(j)
#print global_metrics_manager.cluster_total_utilisation()
print global_metrics_manager.print_global_metrics(global_application_manager.joblist)


#print global_platform_manager.router_scheduler.callcount



"""

thoughts


how to deal with RAM capacity constraints?
especially where many cores and large pool of ram. but process on one core may use all the ram, leaving it inaccessible to the rest of the procs.

model architecurres as eg x_86 low memory, x_86 high memory. low can run on high; but high cannot run on low.
implement architecture as a tuple. how to do quick checking? make method call in each task; hence job; to say 'is this node good for you'.

networking delay constraints

need to add a dependency 'data size' field.
then once a task finishes, add new event when data will be ready on its successor tasks if they are on different resources.

TODO tasks failing, and having to be re-run from some point in their execution - will interrupt given schedule. proc free early but 
probabilities of tasks failing in same place, in different place - work model to decide which is best....

split the schedulers:
one kind for scheduling tasks
one kind for scheduling jobs
make these separate fields in the routers.



rather important.
have not split ordering and allocation. they have been lumped together in an arcane way. 
this model does allocation and then ordering.
trying to get ordering right simply by doing allocation in the right order.

the routers should call an allocator.
the resource managers should call an 'orderer'. 'which task next'. 
actually no. resource managers should call an orderer that can produce a monotonically increasing and unique 'priority'.
the tasks should then be queued on the resource using the in-built resource priority queue.



actually 3 separate 'policies'

consideration order - which tasks should be considered for scheduling first_of
allocation - which proc should each task be allocated to
processing order - what order should allocated tasks be run in?



"""


