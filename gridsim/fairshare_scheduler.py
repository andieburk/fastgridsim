#fair share scheduler .py

import re
import random
from app_man import TaskState
from import_manager import import_manager
import_manager_local = import_manager()
if import_manager_local.import_trace:
	from SimPy.SimulationTrace import *
else:
	from SimPy.Simulation import *

import collections


class FairshareScheduler(object):
	def __init__(self):
		self.name = "fair_share"
	
		self.shares = {}
		self.percents = {}
		self.user_for_jobid = {}
		self.sim_params = None
		self.platform_manager = None
		self.application_manager = None
		self.platform_core_count = -1
		self.user_path_tasks = {}
		self.last_tick = -1
		self.user_running = {}
		self.priority_cache = {}
		self.user_path_tasks_dict_cache = {}

	def activate(self, simulation_parameters, platform_manager, application_manager):
		self.sim_params = simulation_parameters
		self.platform_manager = platform_manager
		self.application_manager = application_manager
		print "\t\treading"
		self.read_fairshares_from_file()
		print "\t\tsetting"
		self.set_all_path_percents()
		print "\t\tset job users"
		self.set_job_fairshare_paths(self.application_manager.joblist)
		print "\t\tsetting platform params"
		self.platform_core_count = self.platform_core_counter()
		print "\t\tuser path finder...??"
		self.user_path_task_finder()


	def read_fairshares_from_file(self):
		sharesfile = open(self.sim_params["fairshare_tree_filename"], 'r')
		
		shares_file_contents = sharesfile.readlines()
		sharesfile.close()
		
		shares_matcher = re.compile("share")
		
		shares_lines_text = [line for line in shares_file_contents if shares_matcher.match(line)]
		
		for line in shares_lines_text:
			share_parts = line.split()
			self.shares[share_parts[1]] = int(share_parts[2])
		
		self.check_read_fairshares_valid(self.shares)
		
	
	def parent_path(self, path):
		splitpath = path.split('/')
		parent_path = '/'.join(splitpath[:-1])
		return parent_path
	
	def check_read_fairshares_valid(self, sharedict):
	
		invalid_paths = []
		for path in sharedict.keys():
			if len(path.split('/')) > 2:
				if self.parent_path(path) not in sharedict.keys():
					#print "parent path", self.parent_path(path), " not found for", path
					invalid_paths.append("error: parent path"+ self.parent_path(path)+ " not found for"+ path)
					sharedict[self.parent_path(path)] = 1
					
		if len(invalid_paths) > 0:
			#for i in invalid_paths:
			#	print i
			print "error,", len(invalid_paths), "invalid paths found. correcting..."
			
		
		
		

	def path_percent(self, path):
		if len(path.split('/')) <= 1:
			return 1
		else:
			path_shares = self.shares[path]
			p_path = self.parent_path(path)
			total_group_shares = self.group_shares[p_path] #sum([self.shares[s] for s in self.shares.keys() if self.parent_path(s) == self.parent_path(path)])
			path_percent = float(path_shares)/float(total_group_shares) * self.path_percent(p_path)
			return path_percent
	
	def set_all_path_percents(self):
	
		self.group_shares = collections.defaultdict(lambda: 0)
		
		for s in self.shares.keys():
			p_path = self.parent_path(s)
			self.group_shares[p_path] += self.shares[s]

		for s in self.shares.keys():
			self.percents[s] = self.path_percent(s)

	def user_paths(self):
		max_user_path_length = max([len(path.split('/')) for path in self.shares.keys()])
		userpaths = [path for path in self.shares.keys() if len(path.split('/')) == max_user_path_length]
		return userpaths


	def set_job_fairshare_paths(self, joblist):
		
		#userpaths = self.user_paths()
		path_not_found_count = 0
		
		for j in joblist:
			if j.fairshare_path != None or j.fairshare_path != "":
			
				if j.fairshare_path in self.shares.keys():
					self.user_for_jobid[j.name] = j.fairshare_path
				else:
					print "fairshare path", j.fairshare_path, "not found in path list (user_paths)"
					path_not_found_count += 1
					#exit()
			else:
				self.user_for_jobid[j.name] = random.choice(userpaths)
				
		if path_not_found_count > 0:
			print path_not_found_count, "paths not found"
			exit()
		
			

	def currently_running_tasks(self):
		running_tasks = [t for j in self.application_manager.joblist for t in j.tasklist if t.state_manager.state == TaskState.running]
	
	def user_path_task_finder(self):
		
		self.user_path_tasks = collections.defaultdict(lambda : [])
		
		for j in self.application_manager.joblist:
			
			self.user_path_tasks[self.user_for_jobid[j.name]].extend(j.tasklist)
	

		#for path in self.user_paths():
		#	self.user_path_tasks[path] = [t for j in self.application_manager.joblist for t in j.tasklist
		#								  if self.task_user_path(t) == path]

	def user_running_core_count(self, userpath):
		return sum([t.corecount for t in self.user_path_tasks[userpath] if t.state_manager.state == TaskState.running])
	
	def user_running_core_count_faster(self, userpath):
		
		if userpath in self.user_path_tasks_dict_cache:
			user_path_taskids_dict = self.user_path_tasks_dict_cache[userpath]
		else:
			user_path_taskids_dict = dict(((t.taskid, t) for t in self.user_path_tasks[userpath]))
			self.user_path_tasks_dict_cache[userpath] = user_path_taskids_dict
		
		if len(user_path_taskids_dict) < 1:
			return 0
		
		any_task = user_path_taskids_dict.iteritems().next()[1]
		
		all_running_tasks = any_task.parent_job.parent_application_manager.running_tasks
		
		if len(all_running_tasks) < 1:
			return 0
			
		running_and_this_userpath_task_corecount_generator = (t.corecount for t in all_running_tasks if t.taskid in user_path_taskids_dict)
		

		running_core_sum = sum(running_and_this_userpath_task_corecount_generator)
		
		"""
		if running_core_sum > 0:
			running_cores = [t.corecount for t in all_running_tasks if t.taskid in user_path_taskids_dict]

			print all_running_tasks
			for t in all_running_tasks:
				print t.corecount

			print running_cores
			print running_core_sum
		
			#exit()
		"""

		return running_core_sum
		


	def platform_core_counter(self):
		total_core_count = sum([c.get_cluster_core_count() for c in self.platform_manager.clusters])
		return total_core_count

	def user_fair_share_cores(self, path):
		fair_core_count = self.percents[path] * self.platform_core_count
		return fair_core_count
	
	def task_user_path(self, task):
		return self.user_for_jobid[task.parent_job.name]
	
	def task_priority(self, task):
		tpath = self.task_user_path(task)
		if self.last_tick < now():
			self.last_tick = now()
			self.priority_cache = {}
		if  tpath not in self.priority_cache.keys():
			#priority closer to 0 is better! this is so priorities don't tend to infinity!
			priority = float(self.user_running_core_count_faster(tpath)) / float(self.user_fair_share_cores(tpath))
			self.priority_cache[tpath] = priority
			return priority
		else:
			return self.priority_cache[tpath]
	
	def ordered(self, tasklist):
		return sorted(tasklist, key=self.task_priority)

if __name__ == "__main__":
	sim_params = {"fairshare_tree_filename" : "/Volumes/NO NAME/anonymised simulation inputs/hl/anon_fairshare_tree_whole_final.txt"}
	f = FairshareScheduler()
	f.activate(sim_params, None, None)

	
