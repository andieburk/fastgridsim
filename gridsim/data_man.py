"""
old more complex file management

    every cluster will be associated with a data store
    a data store contains a list of files
    
    a file will have a unique ID, a size, and a bunch of other metadata
    
    
    
    "get me file as quickly as possible"


    
    
"""

"""

simple network delay model

when a task has completed, record which cluster it executed on.




if a preceeding task finishes, and it is on another cluster, 


in cluster update.

take ready task lists

filter out any tasks that aren't ready /on that cluster/, because their delay hasn't finished yet for other clusters.

for t in tasklist:
	if ready_on_this_cluster_time_cache[t] exists:
		return that >= now()
	else
		t = ready on this cluster time
		cache the time
		return that >= now()

ready_on_this_cluster_time(this_cluster, task):

take into account
level 
computation to communication ratio










"""


"""

how to calculate minimum critical path over the whole platform:

how to find minimum number of transitions across networks.


	def up_exec_time(self, job):
		#assumes tasklist is topologically sorted.
		task_up_exec_times = {}
		
		for t in reversed(job.tasklist):
			
			if len(t.get_dependents()) > 0:
				up_max = max([task_up_exec_times[t2.taskid] for t2 in t.get_dependents()])
			else:
				up_max = 0
			task_up_exec_times[t.taskid] = t.exectime + up_max
			
		critical_path = max(task_up_exec_times.values())
		return critical_path, task_up_exec_times
		
		



"""



class network_delay_manager(object):
	def __init__(self, simulation_parameters, platform_manager):
		self.cluster_path_dict = self.cluster_path_dict_creator(platform_manager.toprouter.name, platform_manager.toprouter.sublist)
		self.cluster_pair_delay_cache = {}
		self.kind_pair_delay_cache = {}
		self.level_factor = simulation_parameters["network_delay_level_power"]
		
		

	def up_exec_time_with_network_delay(self, job, include_network = False, platform_manager = None):
		#assumes tasklist is topologically sorted.
		task_up_exec_times = {}
		
		for t in reversed(job.tasklist):
			
			t_dependents = t.get_dependents()
			
			if len(t_dependents()) > 0:
					
				up_max = max([task_up_exec_times[t2.taskid] for t2 in t_dependents()])
				
				this_task_kind = set([t.requirements_kind])
				dependent_kinds = set([dep_task.requirements_kind for dep_task in t_dependents()])
				
				other_kinds = dependent_kinds - this_task_kind
				
				if len(other_kinds) > 0:
					network_delay = max([kind_difference_minimum_network_delay(this_task_kind[0], k, platform_manager)
									 for k in list(other_kinds)])
					up_max += network_delay
				
			else:
				up_max = 0
			task_up_exec_times[t.taskid] = t.exectime + up_max
			
		critical_path = max(task_up_exec_times.values())
		return critical_path, task_up_exec_times
		


	def clusters_for_kind(self, kindid, platform_manager):
		return [k for k in platform_manager.clusters if kindid in set(k.get_kinds())]
	
	
	
	def cluster_path_dict_creator(self, curr_path, curr_sublist):
		cluster_path_dict_part = {}
		if len(curr_sublist) == 0:
			return {}
		
		if type(curr_sublist[0])[-9:-2] == "Cluster":
			for s in curr_sublist:
				cluster_path_dict_part[s.cluster_id] = curr_path+"/"+s.name
		elif type(curr_sublist[0])[-8:-2] == "Router":	
			for s in curr_sublist:
				sub_dict = cluster_path_dict_creator(curr_path+"/"+s.name, s.sublist)
				for i in sub_dict.items():
					cluster_path_dict_part[i[0]] = i[1]
		else:
			print "error: invalid type", type(curr_sublist[0]), "in cluster path dict creator"
			exit()
		
		return cluster_path_dict_part


	def network_delay_between_two_clusters(cluster1, cluster2, platform_manager):
		
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
			p2split = cluster1path.split('/')
			
			lens = [len(p1split), len(p2split)]
			
			maxdepth = max(lens)
			mindepth = min(lens)
			
			top_depth_to_equal = None
			
			for depth in range(mindepth):
				if p1split[depth] == p2split[depth]:
					top_depth_to_equal = depth
					
			
			bottom_depth_to_equal = maxdepth - top_depth_to_equal
			
			network_delay = self.network_level_delay(bottom_depth_to_equal, self.level_factor)
			
			self.cluster_pair_delay_cache[(cluster1.name, cluster2.name)] = network_delay
			
			return network_delay

	def network_level_delay(self, level, level_factor):
		return level ** level_factor


	def kind_difference_minimum_network_delay(self, sender_kind, receiver_kind, platform_manager):
		possible_sending_clusters = set(self.clusters_for_kind(sender_kind, platform_manager))
		possible_receiving_clusters = set(self.clusters_for_kind(receiver_kind, platform_manager))
		
		if len(possible_sending_clusters.intersection(possible_receiving_clusters)) == 0:
			return 0
		
		elif (sender_kind, receiver_kind) in self.kind_pair_delay_cache.keys():
			return self.kind_pair_delay_cache[(sender_kind, receiver_kind)]
		
		else:
			network_delays = [network_delay_between_two_clusters(c1, c2, platform_manager) for c1 in possible_sending_clusters for c2 in possible_receiving_clusters]
			min_network_delay = min(network_delays)
			
			self.kind_pair_delay_cache[(sender_kind, receiver_kind)] = min_network_delay
			
			return min_network_delay
		



	def minimum_critical_path_for_platform(self, global_platform_manager, job):
		kinds = set([t.requirements_kind for t in job.tasklist])
		if len(kinds) <= 1:
			#return usual critical path








import helpers
import collections
from import_manager import import_manager
import_manager_local = import_manager()
if import_manager_local.import_trace:
	from SimPy.SimulationTrace import *
else:
	from SimPy.Simulation import *
    
    
    


class file_manager(object):
    def __init__(self):
        self.all_file_list = {}
        self.file_id_list = set([])
        self.file_id_counter = helpers.idcounter()
    
    
    def new_file(self, filesize):
        new_file_id = self.file_id_counter.newid()
        nf = file(new_file_id, filesize)
        self.all_file_list[new_file_id] = nf
        self.file_id_list.add(new_file_id)
        return nf
        
    def knows_about_fileid(fileid):
        return fileid in self.file_id_list
    
    def get_file_from_id(fileid):
        if not self.knows_about_fileid(fileid):
            print "error, unknown file id"
            exit()
        else:
            return self.all_file_list[fileid]

    def print_self(self):
        print "all file list:", self.all_file_list
        print "list of file ids", self.file_id_list








class file(object):
    def __init__(self, fileid, filesize):
        self.fileid = fileid
        self.filesize = filesize
        self.metadata = collections.defaultdict(lambda x : None)
        

    def set_metadata_attribute(self, attribute, value):
        self.metadata[attribute] = value
    
    def set_metadata_attributes(self, attribute_value_list):
        for att_value_pair in attribute_value_list:
            self.metadata[att_value_pair[0]] = att_value_pair[1]
    
    def print_self(self):
        print "file id:", self.fileid
        print "file size:", self.filesize
        print "metadata:", self.metadata





class data_store(object):
    def __init__(self, id, manager):
        self.id = id
        self.file_manager = manager
        self.file_list = set([])

    def file_present(self, fileid):
        return fileid in self.file_list

    def add_file(self, fileid):
        self.file_list.add(fileid)

    def delete_file(self, fileid):
        self.file_list.remove(fileid)
    
    def file_from_id(self, fileid):
        return self.file_manager.get_file_from_id(fileid)


"""

#the original dependency manager class that was embedded inside the task class

class dep_man(object):
    def __init__(self, dependencies, dependents):
        self.dependencies = dependencies
        self.dependents = dependents
        self.dependency_names = [t.taskid for t in self.dependencies]
        self.dependency_finished_array = numpy.array([False for t in self.dependencies])
        self.dependencies_satisfied_cached = False
        
    def dependency_completed(self, task):
        ind = self.dependency_names.index(task.taskid)
        self.dependency_finished_array[ind] = True
    
    def dependencies_satisfied(self):
        if self.dependencies_satisfied_cached:
            return True
        else:	
            result = numpy.all(self.dependency_finished_array)
            self.dependencies_satisfied_cached = result
            return result
"""















class dependency_manager_with_data(object):
    def __init__(self, parent_task, dependencies, dependents):
        self.parent_task = parent_task
		self.dependencies = dependencies #list of tasks
		self.dependents = dependents #list of tasks
		self.dependency_names = [t.taskid for t in self.dependencies]
		self.dependency_finished_array = numpy.array([False for t in self.dependencies])
        self.transfers = []
        self.target_cluster = None
        
        if len(dependencies) == 0:
            self.dependencies_satisfied_cached = True
            self.transfers_completed_cached = True
        else:
            self.dependencies_satisfied_cached = False
            self.transfers_completed_cached = False
          

        

			
	def dependency_completed(self, task):
		ind = self.dependency_names.index(task.taskid)
		self.dependency_finished_array[ind] = True
		self.dependencies_satisfied_cached = numpy.all(self.dependency_finished_array)        
        
        
        
		
	def dependencies_satisfied(self):
        return self.dependencies_satisfied_cached and self.transfers_completed_cached

    def set_cluster(self, cluster):
    
        """
        if transfers are pending because the cluster isn't known yet
        start them
        
        otherwise, just set the cluster
        
        
        """
    
        pass
        
    
    def start_transfer(self, individual_dependency):
        pass
    
    def start_all_transfers_for_dependencies(self, source_task, destination_task):
        pass
        
    
    
    
    def task_ready(self, cluster):
        if self.target_cluster == None:
            self.set_cluster(cluster)
        return self.dependencies_satisfied()
        
    
    
        """
        if the task state is ready,
        
        if there are any data transfer dependencies,
        
        
        are the files already in the local cluster?
        if not, start transferring them from wherever they came from
        and tell me when the transfers are finished (using the data transfer kernel)
        
        
        
        have all the data transfer dependencies also completed? (is the data ready as well)?
        
        
        
        """

class data_transfer_kernel(Process)
"""
the actual process that is kicked off as soon as the first dependency is satisfied.

whenever a dependency is satisfied, kick off the transfers

when all the transfers are complete, put this into the local state

"""
    
def completed_task_exec_clusterid(task):
"""
check the task is actually completed

go down to the kernel and find where the task ran

hence find out what kernel, what resource and what cluster it was on when it ran.

get the cluster id and return it

"""    
    

class single_dependency(object):
    def __init__(self, preceeding_task, succeeding_task, file_list):
        self.satisfied = False
        self.preceeding_task = preceeding_task
        self.succeeding_task = succeeding_task
        

class data_movement_manager(object):
    def __init__(self):
        deps_file_list = collections.defaultdict(lambda x : None)
    
    def set_task_pair_file_list(preceeding_task, succeeding_task, files_in_dependency):
        deps_file_list[(preceeding_task.taskid, succeeding_task.taskid)] = files_in_dependency
    
    







if __name__ == "__main__":
    print "hello world"
    
    fman = file_manager()
    
    f1 = fman.new_file(2)
    f1.set_metadata_attribute("owner", "andy")
    
    f1.print_self()



