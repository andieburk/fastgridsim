#schedule visualisation

from metrics import job_metrics
from helpers import *
import numpy
import pylab
import matplotlib.pyplot as plt
import math


class task_viewer(object):
	def __init__(self, task):
		self.task = task

	def get_requirements_kind(self):
		return self.task.requirements_kind
	
	def get_core_count(self):
		return len(self.task.kernels)
	
	def get_dependencies(self):
		return self.task.dependency_manager.dependencies
		
	def get_dependents(self):
		return self.task.dependency_manager.dependents
	
	def get_execution_time(self):
		return self.task.exectime
	
	def start_time(self):
		return self.task.starttime
	
	def finish_time(self):
		return self.task.finishtime
	
	def parent_job_id(self):
		return self.task.parent_job.name
	
	def get_exec_node_name(self):
		return self.get_exec_node().name
	
	def get_exec_node(self):
		if len(self.task.execnodes) > 1:
			print "error - trying to ask for a single exec node for a multicore task"
			exit()
		else:
			return self.task.execnodes[0]
	
	def get_exec_nodes(self):
		return self.task.execnodes

	def get_exec_node_names(self):
		return [execnode.name for execnode in self.task.execnodes]




class schedule_visualiser(object):
	def __init__(self, simulation_parameters):
		self.sim_params = simulation_parameters	
	
	def print_useful_metrics(self, job):
		jmetrics_calc = job_metrics()
		print "\n----\n\n"
		print job.jobid
		print "was submitted at ", jmetrics_calc.job_submission_time(job)
		print "started executing at", jmetrics_calc.job_start_executing_time(job)
		print "finished executing at", jmetrics_calc.job_finish_executing_time(job)
		print "therefore was in flight for", jmetrics_calc.job_in_flight_time(job)
		print "and had a turnaround time of", jmetrics_calc.job_submit_to_finish_time(job)
		print "its critical path length is", jmetrics_calc.job_critical_path_length(job)
		print "giving a ratio of turnaround time/critical path length of", jmetrics_calc.job_turnaround_over_critical_path_ratio(job)
	
	
	def print_metrics_for_all_jobs(self, joblist):
		for j in joblist:
			self.print_useful_metrics(j)
	
	
	def simple_vis(self, joblist, resource_list):
		overall_finish_time = max([t.finishtime for j in joblist for t in j.tasklist])
		print "overall_finish_time", overall_finish_time
	
	def task_name_to_colour(self, task, job_name_to_colour):
		if "visualise_each_task" in self.sim_params.keys() and \
		    self.sim_params["visualise_each_task"]:
				numjobs = len(job_name_to_colour.keys())
				num_tasks = len(task.parent_job.tasklist)
				task_fraction = 0.33 / float(numjobs * num_tasks)
				task_colour_fraction = task_fraction * [t.taskid for t in task.parent_job.tasklist].index(task.taskid)
				task_colour = job_name_to_colour[task.parent_job.name] + task_colour_fraction
				return task_colour			
		else:
			return job_name_to_colour[task.parent_job.name]
	
	
	
	
	def visualise_slr_by_exec_time(self, joblist):
		jmetrics_calc = job_metrics()
		
		job_slrs = []
		job_cps = []
		
		small_jobs = []
		medium_jobs = []
		big_jobs = []
		
		
		
		
		
		for j in joblist:
			
			cp = jmetrics_calc.job_critical_path_length(j)
			slr = jmetrics_calc.slr(j)
			
			if cp <= 3600 * 4:
				small_jobs.append(numpy.float64(slr))
			elif cp <= 3600 * 24:
				medium_jobs.append(numpy.float64(slr))
			else:
				big_jobs.append(numpy.float64(slr))
		
			
			
			
			if slr > 1.0:
		
				job_slrs.append(numpy.float64(slr))
				job_cps.append(numpy.float64(cp))
		
		sj = numpy.array(small_jobs)
		mj = numpy.array(medium_jobs)
		bj = numpy.array(big_jobs)
		
		
		print "mean, median slr for small jobs:", numpy.mean(sj), numpy.median(sj)
		print "mean, median slr for medium jobs:", numpy.mean(mj), numpy.median(mj)
		print "mean, median slr for big jobs:", numpy.mean(bj), numpy.median(bj)
		
		
			
		slrs = numpy.array(job_slrs)
		cps = numpy.array(job_cps)
		
		#print cps[1200:1300]
		#print slrs[1200:1300]
		
		log_slrs = numpy.log(slrs) * 3.0
		log_cps = numpy.log(cps)
		
		
		hist_data, xedges, yedges = numpy.histogram2d(log_cps, log_slrs, bins=[256,256])
		
		import matplotlib.pyplot as plt
		
		
		ab = plt.plot(sorted(log_slrs))
		
		plt.show()
		
		
		#extent = [yedges[0], yedges[-1], xedges[-1], xedges[0]]
		#bb = plt.imshow(hist_data, extent=extent, interpolation='nearest')
		#bb.set_cmap('spectral')
		
		#plt.colorbar()
		#plt.show()
	
	
	
	
	def visualise_cumulative_completion(self, joblist, resource_list):
		
		cumul_completion = 0
		
		ccs = [0]
		finishes = [0]
		
		for j in sorted(joblist, key=lambda j: self.jmetrics_calc.job_finish_executing_time(j)):
			jtexec = self.jmetrics_calc.job_sum_of_task_exec_times(j)
			jfinish = self.jmetrics_calc.job_finish_executing_time(j)
			
			cumul_completion += jtexec
			
			ccs.append(cumul_completion)
			finishes.append(jfinish)
		
		import matplotlib.pyplot as plt
		
		plt.scatter(ccs, finishes)
		
		plt.show()
			
	
	
	
	
	
	def visualise_huge_schedule(self, joblist, resource_list):
		
		graph_pixel_width = len(resource_list) * 4
		
		sort_pixels = True
		
		resource_names = [r.name for r in resource_list]
		num_resources = len(resource_names)
		rname_to_id = {}
		for i in range(num_resources):
			rname_to_id[resource_names[i]] = i

		num_jobs = len(joblist)
		job_name_to_colour = {}
		for i in range(num_jobs):
			job_name_to_colour[joblist[i].name] = float(i+1)/float(num_jobs)
		
		print "calcuating start and finish times"
		overall_finish_time = max([t.finishtime for j in joblist for t in j.tasklist])
		overall_start_time = min([t.starttime for j in joblist for t in j.tasklist])
		
		print overall_start_time, overall_finish_time
		
		print "populating array"
		vis_sched_arr = numpy.zeros((num_resources, graph_pixel_width))# overall_finish_time-overall_start_time))
		
		workload_makespan_x = overall_finish_time - overall_start_time
		
		for j in joblist:
			for t in j.tasklist:
				tv = task_viewer(t)
				task_start_time = tv.start_time() - overall_start_time
				task_finish_time = tv.finish_time() - overall_start_time
				#execnodeid = rname_to_id[tv.get_exec_node_name()]
				execnodeids = [rname_to_id[x] for x in tv.get_exec_node_names()]
				
				task_colour = self.task_name_to_colour(t, job_name_to_colour)
				task_start_pixel = int(math.floor(graph_pixel_width * (float(task_start_time) / float(workload_makespan_x))))
				task_end_pixel = int(math.ceil(graph_pixel_width * (float(task_finish_time) / float(workload_makespan_x))))
				
				for e in execnodeids:
					vis_sched_arr[e][task_start_pixel : task_end_pixel] = task_colour
					
	
		print "plotting"
	
		if sort_pixels:
			vis_sched_arr = numpy.sort(vis_sched_arr, axis=0)
		
		#extent = [yedges[0], yedges[-1], xedges[-1], xedges[0]]
		bb = plt.imshow(vis_sched_arr,  interpolation='nearest')#, extent=extent, interpolation='nearest')
		bb.set_cmap('spectral')
		
		#plt.colorbar()
		plt.show()	
		

			
	
	def visualise_schedule_new(self, joblist, resource_list):
		
		resource_names = [r.name for r in resource_list]
		num_resources = len(resource_names)
		rname_to_id = {}
		for i in range(num_resources):
			rname_to_id[resource_names[i]] = i

		num_jobs = len(joblist)
		job_name_to_colour = {}
		for i in range(num_jobs):
			job_name_to_colour[joblist[i].name] = float(i+1)/float(num_jobs) 
		
		print "calcuating start and finish times"
		overall_finish_time = max([t.finishtime for j in joblist for t in j.tasklist])
		overall_start_time = min([t.starttime for j in joblist for t in j.tasklist])
		
		

				
		print "populating array"
		vis_sched_arr = numpy.zeros((num_resources, overall_finish_time-overall_start_time))
		for j in joblist:
			for t in j.tasklist:
				tv = task_viewer(t)
				task_start_time = tv.start_time()
				task_finish_time = tv.finish_time()
				#execnodeid = rname_to_id[tv.get_exec_node_name()]
				execnodeids = [rname_to_id[x] for x in tv.get_exec_node_names()]
				
				task_colour = self.task_name_to_colour(t, job_name_to_colour)
				
				for e in execnodeids:
					vis_sched_arr[e][task_start_time : task_finish_time] = task_colour
		
		
		print "simplifying array"
		if self.sim_params["faster_less_accurate_charting"] and overall_finish_time > 1024:
			ten_times_smaller = numpy.zeros((num_resources, (overall_finish_time - overall_start_time)/self.sim_params["faster_charting_factor"]))
			
			for i in range((overall_finish_time - overall_start_time)/self.sim_params["faster_charting_factor"]):
				for rid in range(num_resources):
					ten_times_smaller[rid][i] = numpy.median(vis_sched_arr[rid][i*self.sim_params["faster_charting_factor"]:i*self.sim_params["faster_charting_factor"]+self.sim_params["faster_charting_factor"]-1])
			
			vis_sched_arr = ten_times_smaller
		
		print "plotting"
		c = pylab.pcolor(vis_sched_arr)

		pylab.title = 'schedule'
		
		pylab.show()
		
		
	#def visualise_overall_utilisation(self, joblist, resource_list):
		
		
		
		
		
		
		
		
		
		
		
		
		
	

	def visualise_schedule(self,joblist, resource_list):
		
		schedule = {}
		s2 = {}
		resource_names = [r.name for r in resource_list]
		
		for rn in resource_names:
			schedule[rn] = []
			s2[rn] = []
		

		for j in joblist:
			for t in j.tasklist:
				tv = task_viewer(t)
				tinfo = [t.starttime, t.taskid, t.parent_job.jobid, t.finishtime]
				if tv.get_exec_node_name() in schedule.keys():
					schedule[tv.get_exec_node_name()].extend([tinfo])
					s2[tv.get_exec_node_name()].extend([t.parent_job.jobid] * (t.finishtime - t.starttime))
				else:
					schedule[tv.get_exec_node_name()] = [tinfo]
					s2[tv.get_exec_node_name()] = [t.parent_job.jobid] * (t.finishtime - t.starttime)
		
		for execnode in schedule.keys():
			sorted_list = sorted(schedule[execnode], key = first_of)
			schedule_list = []
			for t in sorted_list:
				schedule_list.extend([str(t[0]), str(t[1]), str(t[2][:-1]), str(t[3]), '\n'])
			
			schedule_string = ' '.join(schedule_list)
			print execnode + ':\n' + schedule_string
		
		print schedule.keys()
		maxtime = max([t.finishtime for t in j.tasklist for j in joblist])
		
		vis_sched = [([0] * maxtime) for n in range(len(schedule.keys()))]
		
		for j in joblist:
			for t in j.tasklist:
				tv = task_viewer(t)
				vis_sched[schedule.keys().index(tv.get_exec_node_name())][t.starttime : t.finishtime] = [t.parent_job.jobid] * (t.finishtime - t.starttime)
		
		print len(vis_sched)
		print max([len(vis_sched[i]) for i in range(len(vis_sched))])

		
		vis_sched_arr = numpy.zeros((len(vis_sched),max([len(vis_sched[i]) for i in range(len(vis_sched))])))
		exit()	
		for i in range(len(vis_sched)):
			for j in range(len(vis_sched[i])):
				vis_sched_arr[i][j] = vis_sched[i][j]

		#pylab.subplot(2,1,1)

		c = pylab.pcolor(vis_sched_arr)

		pylab.title = 'schedule'
		
		pylab.show()
		
		