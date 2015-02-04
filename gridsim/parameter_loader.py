#parameter loader .py

import os.path
from fairshare_scheduler import *
from schedulers import *

class scheduler_loader(object):
	def __init__(self):
		self.proj_slr_orderer = None
		self.srtf_orderer = None
		self.lrtf_orderer = None
		self.proj_value_orderer = None
		self.proj_value_density_orderer = None
		self.texec_sum_value_density_orderer = None
		self.projected_value_remaining_orderer = None
		self.pslr_sf_orderer = None
		self.pvd_sq_orderer = None
		
	
	def init_schedulers_phase_one(self, simulation_parameters, fairshare_manager):
		self.cl_orderer_string = simulation_parameters["cluster_orderer"]
		self.router_orderer_string = simulation_parameters["router_orderer"]

		simulation_parameters["cluster_orderer"] = self.name_to_orderer(simulation_parameters["cluster_orderer"], fairshare_manager)
		simulation_parameters["router_orderer"] = self.name_to_orderer(simulation_parameters["router_orderer"], fairshare_manager)
		simulation_parameters["cluster_allocator"] = self.name_to_allocator(simulation_parameters["cluster_allocator"])
		simulation_parameters["router_allocator"] = self.name_to_allocator(simulation_parameters["router_allocator"])
	
	def init_schedulers_phase_two(self, simulation_parameters, joblist, network_manager = None):
		if self.cl_orderer_string == "projected_slr" or \
		   self.router_orderer_string == "projected_slr" :
			self.proj_slr_orderer.initialise_with_workload(simulation_parameters, joblist, network_manager)
		
		if self.cl_orderer_string[:24] == "shortest_remaining_first" or \
			self.router_orderer_string[:24] == "shortest_remaining_first" :
				self.srtf_orderer.initialise_with_workload(simulation_parameters, joblist, network_manager)
	
		if self.cl_orderer_string[:23] == "longest_remaining_first" or \
			self.router_orderer_string[:23] == "longest_remaining_first" :
				self.lrtf_orderer.initialise_with_workload(simulation_parameters, joblist, network_manager)

		if self.cl_orderer_string[:24] == "projected_value_straight" or \
			self.router_orderer_string[:24] == "projected_value_straight" :
				self.proj_value_orderer.initialise_with_workload(simulation_parameters, joblist, network_manager)
	
		if self.cl_orderer_string[:23] == "projected_value_density" or \
			self.router_orderer_string[:23] == "projected_value_density" :
				self.proj_value_density_orderer.initialise_with_workload(simulation_parameters, joblist, network_manager)

		if self.cl_orderer_string[:23] == "texec_sum_value_density" or \
			self.router_orderer_string[:23] == "texec_sum_value_density" :
				self.texec_sum_value_density_orderer.initialise_with_workload(simulation_parameters, joblist, network_manager)

		if self.cl_orderer_string[:25] == "projected_value_remaining" or \
			self.router_orderer_string[:25] == "projected_value_remaining" :
				self.projected_value_remaining_orderer.initialise_with_workload(simulation_parameters, joblist, network_manager)

		if self.cl_orderer_string == "pslr_sf" or \
			self.router_orderer_string == "pslr_sf" :
				self.pslr_sf_orderer.initialise_with_workload(simulation_parameters, joblist, network_manager)

		if self.cl_orderer_string[:6] == "pvd_sq" or \
			self.router_orderer_string[:6] == "pvd_sq" :
				self.pvd_sq_orderer.initialise_with_workload(simulation_parameters, joblist, network_manager)










	def name_to_orderer(self, orderer_name, fairshare_manager):
		if orderer_name == "fifo_job":
			return fifo_job_orderer()
			
		elif orderer_name == "fifo_task":
			return fifo_task_orderer()
		
		elif orderer_name == "random":
			return random_orderer()
		
		elif orderer_name == "identity":
			return identity_orderer()
		
		elif orderer_name == "fair_share":
			return fairshare_manager
		
		elif orderer_name == "projected_slr":
			if self.proj_slr_orderer == None:
				self.proj_slr_orderer = projected_generic_orderer("projected_slr", "projected_slr")
			return self.proj_slr_orderer
		
		elif orderer_name == "pslr_sf":
			if self.pslr_sf_orderer == None:
				self.pslr_sf_orderer = projected_generic_orderer("pslr_sf", "pslr_sf")
			return self.pslr_sf_orderer
		
		elif orderer_name == "shortest_remaining_first":
			if self.srtf_orderer == None:
				self.srtf_orderer = shortest_remaining_first_orderer()
			return self.srtf_orderer	
		
		elif orderer_name == "longest_remaining_first":
			if self.lrtf_orderer == None:
				self.lrtf_orderer = longest_remaining_first_orderer()
			return self.lrtf_orderer

		elif orderer_name == "projected_value_straight_down":
			if self.proj_value_orderer == None:
				self.proj_value_orderer = projected_generic_orderer("projected_value", "projected_value_straight_down", backward=True)
			return self.proj_value_orderer

		elif orderer_name == "projected_value_straight_up":
			if self.proj_value_orderer == None:
				self.proj_value_orderer = projected_generic_orderer("projected_value", "projected_value_straight_up", backward=False)
			return self.proj_value_orderer
	
		elif orderer_name == "projected_value_density_down":
			if self.proj_value_density_orderer == None:
				self.proj_value_density_orderer = projected_generic_orderer("projected_value_density", "projected_value_density_down", backward=True)
			return self.proj_value_density_orderer

		elif orderer_name == "projected_value_density_up":
			if self.proj_value_density_orderer == None:
				self.proj_value_density_orderer = projected_generic_orderer("projected_value_density", "projected_value_density_up", backward=False)
			return self.proj_value_density_orderer


		elif orderer_name == "texec_sum_value_density_down":
			if self.texec_sum_value_density_orderer == None:
				self.texec_sum_value_density_orderer = projected_generic_orderer("projected_value_density", "texec_sum_value_density_down", backward=True, sum_not_cp=True)
			return self.texec_sum_value_density_orderer


		elif orderer_name == "texec_sum_value_density_up":
			if self.texec_sum_value_density_orderer == None:
				self.texec_sum_value_density_orderer = projected_generic_orderer("projected_value_density", "texec_sum_value_density_up", backward=False, sum_not_cp=True)
			return self.texec_sum_value_density_orderer


		elif orderer_name == "projected_value_remaining_down":
			if self.projected_value_remaining_orderer == None:
				self.projected_value_remaining_orderer = projected_generic_orderer("projected_value_remaining", "projected_value_remaining_down", backward=True)
			return self.projected_value_remaining_orderer

		elif orderer_name == "projected_value_remaining_up":
			if self.projected_value_remaining_orderer == None:
				self.projected_value_remaining_orderer = projected_generic_orderer("projected_value_remaining", "projected_value_remaining_up", backward=False)
			return self.projected_value_remaining_orderer

		elif orderer_name == "pvd_sq_up":
			if self.pvd_sq_orderer == None:
				self.pvd_sq_orderer = projected_generic_orderer("pvd_sq", "pvd_sq_up", backward=False)
			return self.pvd_sq_orderer

		elif orderer_name == "pvd_sq_down":
			if self.pvd_sq_orderer == None:
				self.pvd_sq_orderer = projected_generic_orderer("pvd_sq", "pvd_sq_down", backward=True)
			return self.pvd_sq_orderer

		
		
		



		else:
			print "unrecognised orderer", orderer_name
			exit()
		
	def name_to_allocator(self, allocator_name):
		if allocator_name == "first_fit" or allocator_name == "first_free":
			return first_free_allocator()
		elif allocator_name == "random":
			return random_allocator()
		elif allocator_name == "load_balancing_allocator":
			return load_balancing_allocator()
		else:
			print "error - invalid allocator name: ", allocator_name
			exit()
	

class parameter_loader(object):
	def get_params(self, params_filename):
		f = open(params_filename)
		lines = f.readlines()
		line = ','.join([l.strip() for l in lines if l[0] != '#'])
		final_dict_line = '{' + line + '}'
		#print final_dict_line
		full_dict = eval(final_dict_line)
		full_dict["params_filename"] = os.path.basename(params_filename)
		
		
		if full_dict["networking_enabled"]:

			if full_dict["timeouts_enabled"]:
				print "****** warning: *hack* present. turning off timeouts because networking enabled."
				full_dict["timeouts_enabled"] = False
		
		
		f.close()
		return full_dict
		
		
		
		
		
if __name__ == "__main__":
	p = parameter_loader()
	params = p.get_params("example_simulation_parameters1.py")
	print len(params.keys())
	print params["debugging_on"]
	print params


