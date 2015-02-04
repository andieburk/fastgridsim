#scheduler initialiser.py







#Note ========== this file is dead now ========= subsumed into parameter_loader.py







from fairshare_scheduler import *
from schedulers import *

class scheduler_loader(object):

	def init_schedulers(self, simulation_parameters, joblist):
		
		if simulation_parameters["cluster_orderer"] == "projected_slr" or \
		   simulation_parameters["router_orderer"]  == "projected_slr" :
		
			self.proj_slr_orderer = projected_slr_orderer()
			
			self.proj_slr_orderer.initialise_with_workload(simulation_parameters, joblist)
		else:
			pass
			
		
	def name_to_orderer(self, orderer_name):
		if orderer_name == "fifo_job":
			return fifo_job_orderer()		
		
		elif orderer_name == "identity":
			return identity_orderer()
		
		elif orderer_name == "random":
			return random_orderer()
		
		elif orderer_name == "fifo_task":
			return fifo_task_orderer()
		
		elif orderer_name == "fair_share":
			return FairshareScheduler()
		
		elif orderer_name == "projected_slr":
			return self.proj_slr_orderer()
		
		elif orderer_name == "shortest_remaining_first":
			return shortest_remaining_first_orderer()

		elif orderer_name == "longest_remaining_first":
			return longest_remaining_first_orderer()
					
			
			
			
	def name_to_allocator(self, allocator_name):
		if allocator_name == "first_fit":
			return first_fit_allocator()
		elif allocator_name == "random":
			return random_allocator()
		else:
			print "error - invalid allocator name: ", allocator_name
			exit()
	