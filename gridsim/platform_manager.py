# -*- coding: utf-8 -*-
""" platform_manager.py
"""

import re
from req_man import requirements_manager
from resource_man import ResourceKernel, Cluster, Router
from import_manager import import_manager
import_manager_local = import_manager()
if import_manager_local.import_trace:
	from SimPy.SimulationTrace import *
else:
	from SimPy.Simulation import *


class platform_manager_old():
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


class platform_manager(object):
	def __init__(self, requirements_manager, simulation_parameters):
		self.requirements_manager = requirements_manager
		self.sim_params = simulation_parameters
		self.resources = []
		self.routers = []
		self.clusters = []
		self.toprouter = None

	def new_clusters_and_resources(self, clusters_resources_list):
		for c in clusters_resources_list:
			res_list = []
			curr_cluster = Cluster(c[0], self.sim_params)
			for r in c[1]:
				newres = ResourceKernel(r[0], curr_cluster, self.sim_params, r[1], self.requirements_manager, curr_cluster.resource_manager_bulk)
				res_list.append(newres)
				self.resources.append(newres)
			curr_cluster.set_resources(res_list)
			self.clusters.append(curr_cluster)
	
	def new_routers(self, routers_list):
	
		#print "routers list", routers_list
		
		new_router_list = []
		
		#print routers_list
		
		for r in routers_list[::-1]:
			sublist = [self.item_by_id(cid) for cid in r[1]]			
			newrouter = Router(r[0], sublist, self.requirements_manager, self.sim_params)
			self.routers.append(newrouter)
		self.routers = self.routers[::-1]
		self.toprouter = self.routers[0] #!!!!! TODO improve this - what order are routers defined? by topmost router
		#print "self routers", [r.name for r in self.routers]

	def item_by_id(self, itemid):

		poss_resources = [r for r in self.resources if r.name == itemid]
		poss_clusters  = [r for r in self.clusters  if r.name == itemid]
		poss_routers   = [r for r in self.routers   if r.name == itemid]
		lens = [len(poss_resources), len(poss_clusters), len(poss_routers)]
		
		#print itemid, "lens", lens
		#print [r.name for r in self.resources]
		#print [r.name for r in self.clusters]
		#print "self routers2", [r.name for r in self.routers]
		
		if sorted(lens) != [0,0,1]:
			print "matching error (non-unique names?)"
			print itemid
			print poss_resources
			print poss_clusters
			print poss_routers
			exit()
		for p in [poss_resources, poss_clusters,poss_routers]:
			if len(p) == 1:
				return p[0]
	
	def get_platform_core_count(self):
		if len(self.clusters) > 0:
			core_count_for_all_clusters = sum([c.get_cluster_core_count() for c in self.clusters])
			return core_count_for_all_clusters
		else:
			print "error, asking for cluster count before clusters set"
			exit()
		
		
	def set_resources(self, new_resource_list):
		self.resources = new_resource_list
		
	def set_clusters(self, new_cluster_list):
		self.clusters = new_cluster_list
		
	def set_routers(self, new_router_list):
		self.routers = new_router_list
	
	def go(self):
		#for rx in self.resources:
		#	activate(rx, rx.go(), at=now())
		for cx in self.clusters:
			activate(cx, cx.go(), at=now())
		for r in self.routers:
			activate(r, r.go(), at=now())



class platform_reader(object):
	
	def parse_file(self, filename):
		kind_matcher = re.compile("Kind")
		cluster_matcher = re.compile("Cluster")
		resource_matcher = re.compile("Resource")
		router_matcher = re.compile("Router")
		
		platform_file = open(filename, 'r')
		platform_lines = platform_file.readlines()
		platform_file.close()
		
		self.parsed_kinds = []
		self.kindname_to_kindid = {}
		self.cluster_list = []
		self.router_list = []
		self.resource_list = []
		
		curr_cluster = None
		
		for line in platform_lines:
			if kind_matcher.match(line):
				#print line
				linesplit = line.split()
				#print linesplit
				kind_name = linesplit[1]
				kind_line_split = linesplit[2].split(";")
				#print kind_name
				#print kind_line_split
				abs_bits = kind_line_split[0].split(',')
				
				if len(kind_line_split) > 1:
					quant_bits = kind_line_split[1].split(',')
				else:
					quant_bits = []
				#print abs_bits
				#print quant_bits
				abs_list = [tuple(abit.split(':')) for abit in abs_bits]
				quant_list = [tuple(qbit.split(':')) for qbit in quant_bits]
				#print abs_list
				#print quant_list
				
				self.parsed_kinds.append((kind_name, [abs_list, quant_list]))
			
			if cluster_matcher.match(line):
				if curr_cluster is not None:
					self.cluster_list.append(curr_cluster)
				curr_cluster = [line.split()[1], []]
			
			if resource_matcher.match(line):
				curr_cluster[1].append(line.split()[1:])
				self.resource_list.append(line.split()[1:])
			
			if router_matcher.match(line):
				self.router_list.append([line.split()[1], line.split()[2:]])
		
		self.cluster_list.append(curr_cluster)
	
	def requirements_from_parsed_file(self):
		newreqman = requirements_manager()
		for k in self.parsed_kinds:
			newkindname = newreqman.new_kind(k[1][0],k[1][1],k[0])
			#print k, "new kind name is", newkindname
		return newreqman
	
	
class platform_manager_constructor(object):
	def construct_requirements_and_platform_from_file(self, sim_params):
		filename = sim_params["platform_filename"]
		reader = platform_reader()
		reader.parse_file(filename)
		reqman = reader.requirements_from_parsed_file()
		platman = platform_manager(reqman, sim_params)
		platman.new_clusters_and_resources(reader.cluster_list)
		platman.new_routers(reader.router_list)
		return reqman, platman
		#working here because the requirements reader isn't storing the expected key correctly (at all?)
		
	
		
		
if __name__ == "__main__":
	from app_man import TaskState

	"""a = platform_reader()
	a.parse_file("platform1.plat")
	print a.parsed_kinds
	print a.cluster_list
	print a.router_list
	print a.resource_list
	newreqman = a.requirements_from_parsed_file()
	print newreqman.kinds"""
	sim_params = {"platform_filename":"platform1.plat", 
				  "debugging_on" : True,
				  "cluster_orderer" : None,
				  "cluster_allocator" : None,
				  "router_orderer": None,
				  "router_allocator": None	
	}
	
	b = platform_manager_constructor()
	platform_man_instance = b.construct_requirements_and_platform_from_file(sim_params)
	print platform_man_instance.sim_params
	print [r.name for r in platform_man_instance.resources]
	print [r.name for r in platform_man_instance.routers]
	print [r.name for r in platform_man_instance.clusters]
	
	
	# loading a platform from file now works. seems like things are indeed read correctly and set up correctly.
	
	#now to set up a simple but whole simulation to run a job through the refactored simulator.
	#then put visualisation into a nice class to analyse the schedules.
	
	
	
	
	
	
	
	
	
	
	

