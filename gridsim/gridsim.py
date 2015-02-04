"""
gridsim.py
the new controller for the refactored and restructured grid simulation framework
Andrew Burkimsher
October 2011
"""

import os
import random
import cProfile

#import numpy
from helpers import *
from parameter_loader import parameter_loader, scheduler_loader
from task_state_new import TaskState
from app_man import task, job, application_manager
from req_man import requirements_manager
from resource_man import ResourceManager, Cluster, Router
from platform_manager import platform_manager, platform_manager_constructor
from schedulers import *
from fairshare_scheduler import FairshareScheduler
from network_delay_manager import network_delay_manager, state_man_with_networking
from metrics import job_metrics, global_metrics, metrics_manager
from timeout_manager import timeout_manager
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
	from schedule_visualisation import schedule_visualiser



def go():


	print "loading simulation parameters"


	if len(sys.argv) < 2:
		print "no parameter file given, using default parameter file in gridsim/example_simulation_parameters1.py"
		os.chdir("/Users/andy/Documents/Work/gridsimxcode/GridSim/gridsim/")

		simulation_parameters_filename = "example_simulation_parameters1.py"
	else:
		simulation_parameters_filename = sys.argv[1]
	
	
	
	

	print "\tsetting up simulation parameters"
	global_parameter_manager = parameter_loader()
	simulation_parameters = global_parameter_manager.get_params(simulation_parameters_filename)

	if "random_seed" in simulation_parameters.keys():
		print "setting random seed to ", simulation_parameters["random_seed"]
		
	else:
		print "random seed set to default, 12346"
		random.seed(12346)



	fairshare_enabled = simulation_parameters["fairshare_enabled"]
	
	if fairshare_enabled:
		print "\tfair share enabled, setting up fairshare parameters"
		global_fairshare_manager = FairshareScheduler()
	else:
		global_fairshare_manager = None
	
	print "initialising components...."

	print '\tinit SimPy library'
	initialize()

	print '\tinit scheduler(s) phase 1'
	#init the scheduling policies if required
	global_scheduler_manager = scheduler_loader()
	global_scheduler_manager.init_schedulers_phase_one(simulation_parameters, global_fairshare_manager)


	print '\tinitialising platform (requirements and platform managers)'
	platform_constructor = platform_manager_constructor()
	global_req_manager, global_platform_manager = \
		platform_constructor.construct_requirements_and_platform_from_file(simulation_parameters)

	#print 'adding task requirements to requirements_manager as unimplemented in read applications from file'
	#nk = global_req_manager.new_kind([("architecture", "x86")],[("ram", 1)])
	#simulation_parameters["task_unspecified_kind"] = nk

	if "networking_enabled" in simulation_parameters.keys():
		if simulation_parameters["networking_enabled"]:
			print '\tinit networking'
			global_networking_manager = network_delay_manager(simulation_parameters, global_platform_manager, global_req_manager)
		else:
			simulation_parameters["networking_enabled"] = False
			global_networking_manager = None
		
	else:
		simulation_parameters["networking_enabled"] = False
		global_networking_manager = None
		
	
	print '\tinit applications'
	#create the application(s)
	global_application_manager = application_manager("global_application_manager", global_platform_manager, simulation_parameters, global_networking_manager)
	global_timeout_manager = timeout_manager("global_timeout_manager", global_application_manager.joblist, simulation_parameters, global_networking_manager)
	global_application_manager.set_timeout_manager(global_timeout_manager)

	print '\tinit scheduler(s) phase 2'
	global_scheduler_manager.init_schedulers_phase_two(simulation_parameters, global_application_manager.get_joblist(), global_networking_manager)

	if fairshare_enabled:
		print '\tinit fairshare'
		global_fairshare_manager.activate(simulation_parameters, global_platform_manager, global_application_manager)
		#print global_fairshare_manager.user_for_jobid
	
	
	print "starting components...."

	print '\tplatform go'
	#let the platform processes go
	global_platform_manager.go()

	print '\tapplications go'
	#let the application processes go

	simulation_start_time = global_application_manager.earliest_job_start_time()

	activate(global_application_manager, global_application_manager.go(), at=simulation_start_time)
	activate(global_timeout_manager, global_timeout_manager.go(), at=simulation_start_time)

	print "Running the simulation..."
	print '\tsimulation go'

	#perform the simulation
	simulate(until = simulation_parameters['simulation_max_time'])
	
	
	print "\nSimulation finished at time", now(), "\n\n"
	print now() / (60.0 * 24.0), "nominal days after start"
	simulation_parameters["sim_finish_days"] = now() / (60.0 * 24.0)

	
	#the guard to check the completeness of the simulation.
	cluster_run_counts = collections.defaultdict(lambda : 0)
	for j in global_application_manager.joblist:
		
		
		
		for t in j.tasklist:
			start_finishes_invalid = t.starttime == None or t.finishtime == None
			timed_out = t.state_manager.state == TaskState.timed_out

			if start_finishes_invalid and not timed_out:
				print "fatal error: simulation has ended but some tasks have not run or timed out:"
				print [p.taskid for jp in global_application_manager.joblist for p in jp.tasklist if p.starttime < 0 and t.state_manager.state != TaskState.timed_out]
				#print [n.taskid for n in global_platform_manager.router_scheduler.task_fifo_queue]
				exit() # <------------ exit if guard fails
			
			if t.state_manager.state == TaskState.completed:
				t_run_cluster = t.get_exec_cluster()
				cluster_run_counts[t_run_cluster.name] += 1




	if simulation_parameters["print_metrics_by_job"]:
		job_metrics_manager = job_metrics(simulation_parameters)
		for j in global_application_manager.joblist:
			print "down", job_metrics_manager.down_exec_time(j)
			print "up", job_metrics_manager.up_exec_time(j)
	
	
	

	if simulation_parameters["print_global_metrics"]:
		print "printing metrics"
		global_metrics_manager = metrics_manager(global_application_manager,
												 global_platform_manager,
												 global_networking_manager,
												 simulation_parameters,
												 now(),
												 simulation_start_time)
		global_metrics_manager.print_global_metrics_all()


	if simulation_parameters["visualisation_enabled"] and not import_manager_local.use_pypy:
		print "visualising simulation results"
		global_schedule_visualiser = schedule_visualiser(simulation_parameters)
		#global_schedule_visualiser.print_metrics_for_all_jobs(global_application_manager.joblist)
		print cluster_run_counts
		
		if (now() < 200) and (len(global_platform_manager.resources) < 100):
			global_schedule_visualiser.visualise_schedule_new(
				[j for j in global_application_manager.joblist if not j.timed_out],
				global_platform_manager.resources)
		else:
			global_schedule_visualiser.visualise_huge_schedule(
				[j for j in global_application_manager.joblist if not j.timed_out],
				global_platform_manager.resources)
			
		#global_schedule_visualiser.visualise_slr_by_exec_time(global_application_manager.joblist)
	
	if simulation_parameters["output_schedule_to_file"]:
		print "exporting schedule to file"
		
		if global_metrics_manager == None:
				global_metrics_manager = metrics_manager(global_application_manager,
														 global_platform_manager,
														 global_networking_manager,
														 simulation_parameters,
														 global_application_manager.sim_halt,
														 simulation_start_time)
		
		global_metrics_manager.write_out_schedule(simulation_parameters["schedule_output_filename"])		






#cProfile.run("go()")

go()

print "\nsimulation run complete."

exit()































"""



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

split the schedulers: - done
one kind for scheduling tasks
one kind for scheduling jobs
make these separate fields in the routers.



rather important. - done and properly re-engineered
have not split ordering and allocation. they have been lumped together in an arcane way. 
this model does allocation and then ordering.
trying to get ordering right simply by doing allocation in the right order.

the routers should call an allocator. - done
the resource managers should call an 'orderer'. 'which task next'. 
actually no. resource managers should call an orderer that can produce a monotonically increasing and unique 'priority'.
the tasks should then be queued on the resource using the in-built resource priority queue.



actually 3 separate 'policies' - done second two. consideration order is arrival order in platform file.

consideration order - which tasks should be considered for scheduling first_of
allocation - which proc should each task be allocated to
processing order - what order should allocated tasks be run in?



"""












