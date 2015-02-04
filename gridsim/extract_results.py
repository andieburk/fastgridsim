import cPickle as pickle
import numpy
import os
import tables
import numpy
import string

scheduler_kinds = tables.Enum([
						'fifo_task',
						'fifo_job',
						'fairshare',
						'longest_remaining_first',
						'shortest_remaining_first',
						'projected_slr',
						'random',
						'first_free',
						'earliest_start_time',
						'load_balancing',
						'first_free_better'
						])

estimate_kinds = tables.Enum(['exact', 'log_round', 'normal_jitter'])

enum_string_to_type ={	'fifo_task' : scheduler_kinds.fifo_task,
						'fifo_job' : scheduler_kinds.fifo_job,
						'fairshare' : scheduler_kinds.fairshare,
						'fair_share' :  scheduler_kinds.fairshare,
						'longest_remaining_first' : scheduler_kinds.longest_remaining_first,
						'shortest_remaining_first' : scheduler_kinds.shortest_remaining_first,
						'projected_slr' : scheduler_kinds.projected_slr,
						'random' : scheduler_kinds.random,
						'first_free' : scheduler_kinds.first_free,
						'earliest_start_time' : scheduler_kinds.earliest_start_time,
						'load_balancing' : scheduler_kinds.load_balancing,
						'first_free_better' : scheduler_kinds.first_free_better,
						'exact' : estimate_kinds.exact,
						'log_round' : estimate_kinds.log_round,
						'normal_jitter' : estimate_kinds.normal_jitter
						}



sched_table_col = tables.EnumCol(scheduler_kinds, dflt='random', base='uint8', shape=())
est_table_col	= tables.EnumCol(estimate_kinds, dflt='exact', base='uint8', shape=())


"""
'dtype='UInt8')
est_table_col	= tables.EnumCol(estimate_kinds, dtype='UInt8')

tables.EnumAtom(enum, dflt, base, shape=())

"""

def dict_from_file(file_path):
		#res_file_path = analysis_aggregate_file_path
		res_file = open(file_path, 'r')
		
		#print file_path
		dict_filled = False
		
		new_res_dict = {}
		#print "reading files in"
		for line in res_file:
			#print line
			if line[0:25] == '#Results_Output#:#Values#':
				if dict_filled == True:
					print "error, multiple outputs in same input file"
				else:
					dict_filled = True
					entries = line.split('|')[1:]

					#types = set([e.split(':')[2] for e in entries])
					
					#new_res_dict = {}
					for e in entries:
						e_parts = e.split(":")
						
						#print e_parts
						
						value = e_parts[1]
						
						incl = True
						
						if e_parts[2] == "int":
							value = int(value)
						elif e_parts[2] == "numpy.float64":
							value = float(value)
						elif e_parts[2] == "float":
							value = float(value)
						elif e_parts[2] == "long":
							value = numpy.float64(value)
						elif e_parts[2] == "list":
							value = eval(value)
						elif e_parts[2] == "str":
							if e_parts[1] in enum_string_to_type.keys():
								value = enum_string_to_type[e_parts[1]]
								
							else:
								value = e_parts[1]
								
						newkey = string.replace(e_parts[0], ' ', '_')
						#print newkey
						new_res_dict[newkey] = value
		
		
		
		
		
		if dict_filled == False:
			print "warning: no dict found in", file_path

		return new_res_dict



def build_table_from_folder_of_files(folder_path, table_filename, text_files = False):
	
	files_to_analyse = os.walk(folder_path).next()[2]
	print "loading", len(files_to_analyse), " files"
	total_files = len(files_to_analyse)
	curr_file_index = 1


	#table_filename = "/Users/andy/Desktop/picktest/new_runlog_table.h5"
	#input_pickle_file = "/Users/andy/Desktop/picktest/pick_runlog_for_example_simulation_parameters1.py.bin"

	master_table_writer = table_writer(table_filename)
	#print text_files
	
	bad_dicts = 0
	total_dicts = 0
	
	for file in files_to_analyse:
		filepath = folder_path + file
		
		total_dicts += 1
		
		#print "loading", filepath
		
		
		if text_files:
			read_dict = dict_from_file(filepath)
		else:
			with open(input_pickle_file, 'rb') as filehandle:
				read_dict = pickle.load(filehandle)
		
		#print read_dict
		if len(read_dict) > 0:
			master_table_writer.new_row_with_pickled_dictionary(read_dict)
		
		else:
			bad_dicts += 1
			#print "error, not read in dict properly"
			#print read_dict
			#exit()
		
	if bad_dicts > 0:
		print "there were", bad_dicts, "files that did not contain results of", total_dicts, "total."





	print master_table_writer.h5file
	print master_table_writer.slr_group.slr_table
	print master_table_writer.run_group.run_table





	
	#for i in range(10):
	#	print files_to_analyse.next()
	
	#print files_to_analyse[0:10]








class run_log_record(tables.IsDescription):
    #runlogid         = tables.UInt32Col()   # 32-bit Unsigned integer
    #mean_slr         = tables.Float32Col() # 64-character String
    #max_slr          = tables.Float32Col() # 64-character String
    #cluster_orderer  = tables.EnumCol(scheduler_kinds, 'fifo_task', base='uint8')


	average_actual_utilisation			= tables.Float32Col()
	cluster_allocator					= sched_table_col
	cluster_orderer						= sched_table_col
	communication_to_computation_ratio	= tables.Float32Col()
	cumulative_completion				= tables.Float64Col()
	cumulative_completion_with_base		= tables.Float64Col()
	cumulative_completion_base			= tables.Int64Col()
	debugging_on						= tables.BoolCol()
#	decile_worst_slr					= not included, can be calculated later from slr values
#	decile_slr_means					= "
#	decile_slr_stddevs					= "
	exec_time_estimate_kind				= est_table_col
	exec_time_estimate_value			= tables.Float32Col()
	fairshare_enabled					= tables.BoolCol()
	fairshare_tree_filename				= tables.StringCol(64) # 64-character String
	faster_charting_factor				= tables.Int32Col()
	faster_less_accurate_charting		= tables.BoolCol()
	flow								= tables.Float32Col()
	gini_coeff_of_slr_inverses			= tables.Float32Col()
	gini_coeff_of_slrs					= tables.Float32Col()
	jain_fairness_of_slr_inverses		= tables.Float32Col()
	jain_fairness_of_slrs				= tables.Float32Col()
#	job_finish_counter					= tables.Int32Col() #an internal class to the implementation. perhaps better to not implement it like this...
	max_job_count						= tables.Int32Col()
	maximum_in_flight_over_cp			= tables.Float32Col()
	maximum_slr							= tables.Float32Col()
	median_slr							= tables.Float32Col()
	median_in_flight_over_cp			= tables.Float32Col()
	mean_speedup						= tables.Float32Col()
	mean_stretch						= tables.Float32Col()
	mean_in_flight_over_cp				= tables.Float32Col()
	mean_slr							= tables.Float32Col()
	minimum_in_flight_over_cp			= tables.Float32Col()
	minimum_slr							= tables.Float32Col()
	network_delay_level_power			= tables.Float32Col()
	networking_enabled					= tables.BoolCol()
	output_schedule_to_file				= tables.BoolCol()
	peak_in_flight						= tables.Int32Col()
	platform_filename					= tables.StringCol(64)
	print_global_metrics 				= tables.BoolCol()
	print_metrics_by_job				= tables.BoolCol()
	proportion_of_kind2					= tables.Float32Col()
	#quartile_slr_means = not included, calcuate later from slr
	#quartile_slr_stddevs
	router_allocator					= sched_table_col
	router_orderer						= sched_table_col
	schedule_output_filename			= tables.StringCol(64)
#	simulation_max_time					= tables.Int64Col()
	staggered_release					= tables.BoolCol()
	stddev_in_flight_over_cp			= tables.Float32Col()
	stddev_speedup						= tables.Float32Col()
	stddev_stretch						= tables.Float32Col()
	stddev_slr							= tables.Float32Col()
	sum_of_excess_slrs					= tables.Float64Col()
	target_utilisation					= tables.Int16Col()
	use_supplied_submit_times			= tables.BoolCol()
	visualisation_enabled				= tables.BoolCol()
	visualise_each_task					= tables.BoolCol()
	workload_makespan					= tables.Int64Col()
	workload_filename					= tables.StringCol(64)
	worst_case_slr						= tables.Float32Col()
	slrs_start_index					= tables.Int64Col()
	slrs_finish_index					= tables.Int64Col()



class slr_record(tables.IsDescription):
	runlogid		= tables.Int32Col()
	slr				= tables.Float32Col()





class table_writer(object):
	def __init__(self, new_table_file):
		
		
		self.h5file = tables.openFile(new_table_file, mode = "w", title = "Sweep information file")

		blosc_filters = tables.Filters(complib='blosc', complevel=9)
		#x = f.createCArray(f.root, "x", atom=atom, shape=(N,), filters=filters)

		self.run_group = self.h5file.createGroup("/", 'run_group', 'group for records for each run')
		self.run_table = self.h5file.createTable(self.run_group, 'run_table', run_log_record, "run_table", filters=blosc_filters)

		self.slr_group = self.h5file.createGroup("/", 'slr_group', 'group for array for the slrs')
		self.slr_table = run_table = self.h5file.createTable(self.slr_group, 'slr_table', slr_record, "slr_table", filters=blosc_filters)

		self.curr_slr_index = 0
		self.curr_runlog_index = 1


	def new_row_with_pickled_dictionary(self, read_dict):
		new_runlog_row = self.run_table.row

		new_runlog_row['slrs_start_index'] = self.curr_slr_index
		
		for s in read_dict["z_all_job_slrs"]:
			new_slr = self.slr_table.row
			new_slr['runlogid'] = self.curr_runlog_index
			new_slr['slr'] = s
			new_slr.append()
		
		self.slr_table.flush()
			
		self.curr_slr_index += len(read_dict["z_all_job_slrs"])
		new_runlog_row['slrs_finish_index'] = self.curr_slr_index
		
		
		new_row = self.fill_row_from_dict(new_runlog_row, read_dict)

		
		new_row.append()
		self.run_table.flush()
		
		self.curr_runlog_index += 1




	def fill_row_from_dict(self, new_runlog_row, read_dict):
		#print self.run_table.description

		excluded_keys =	   ['decile_slr_means',
							'decile_slr_stddevs',
							'decile_worst_slr',
							'median_in_flight_over_cp',
							'median_slr',
							'metrics_out_formats',
							'params_filename',
							'pickle_output_path',
							'quartile_slr_means',
							'quartile_slr_stddevs',
							'z_all_job_slrs',
							'job_finish_counter'
						    ]
							
		excluded_keys_set = set(excluded_keys)
		pytable_keys_set = set([i for i in dir(self.run_table.description) if i[0] != '_'])
		dict_keys_set =  set(read_dict.keys())
		
		extra_keys = dict_keys_set - pytable_keys_set
		
		keys_intersection = dict_keys_set & pytable_keys_set
		
		warnings = False
		
		if warnings:
		
			
			if len(extra_keys - excluded_keys_set) > 0:
				print "error, some keys are being supplied by the input dictionary but are not handled by the pytables reader"
				
				print extra_keys
				
				print "     in dict but not in pytable"
				for i in sorted(extra_keys):
					print i
					
				print "     in pytable but not in dict"
				for i in sorted(pytable_keys_set - dict_keys_set):
					print i
			
		

		"""
		all_keys_set = set(read_dict.keys())
		excluded_keys_set = set(excluded_keys)
		filtered_keys_set = all_keys_set - excluded_keys
		"""
		
		for k in keys_intersection:# pytable_keys_set - set(['slrs_start_index', 'slrs_finish_index']):
			
			new_runlog_row[k] = read_dict[k]

		return new_runlog_row


if __name__ == '__main__':
	
	
	build_table_from_folder_of_files("/Volumes/Red 500 GB/sim_data/netf_outs/",
									 "/Volumes/Red 500 GB/new_runlog_table.h5",
									 True)
	
	"""
	table_filename = "/Users/andy/Desktop/picktest/new_runlog_table.h5"
	input_pickle_file = "/Users/andy/Desktop/picktest/pick_runlog_for_example_simulation_parameters1.py.bin"

	master_table_writer = table_writer(table_filename)

	filehandle = open(input_pickle_file, 'rb')
	read_dict = pickle.load(filehandle)

	master_table_writer.new_row_with_pickled_dictionary(read_dict)

	print master_table_writer.h5file
	print master_table_writer.slr_group.slr_table
	print master_table_writer.run_group.run_table

	filehandle.close()
	"""


"""


a = numpy.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0])
b = numpy.array([4.0, 5.0, 6.0])


atom = tables.Atom.from_dtype(a.dtype)







filehandle = open(input_pickle_file, 'rb')
dict = pickle.load(filehandle)





array1 = h5file.createArray(group2, 'a1', a)
array2 = h5file.createArray(group2, 'b1', b)


x = table.row

x['runlogid'] = 12345
x['mean_slr'] = 1.25
x['max_slr'] = 150.223
x['test'] = array1
#x['cluster_orderer'] = 'fairshare'

x.append()


print h5file

print array1

print h5file.root.decile_mean_slr_arrays.a1

"""
"""
scheduler_kinds = tables.Enum([
						'fifo_task',
						'fifo_job',
						'fairshare',
						'longest_remaining_first',
						'shortest_remaining_first',
						'projected_slr',
						'random',
						'first_free',
						'earliest_start_time',
						'load_balancing',
						'first_free_better',
						'longest_remaining_first' : 12
						])




class run_log_data(tables.IsDescription):
	average_actual_utilisation			= tables.Float32Col()
	cluster_allocator					= tables.EnumCol(scheduler_kinds)
	cluster_orderer						= tables.UInt8Col()
	communication_to_computation_ratio	= tables.Float32Col
	cumulative_completion				= tables.UInt64Col()
	cumulative_completion_with_base		= tables.UInt64Col()
	cumulative_completion_base			= tables.UInt64Col()
	debugging_on						= tables.BoolCol()
	decile_worst_slr					= tables.Float32Col()
	decile_slr_means					= 
	decile_slr_stddevs
	exec_time_estimate_kind
	exec_time_estimate_value
	fairshare_enabled
	fairshare_tree_filename
	faster_charting_factor
	faster_less_accurate_charting
	flow
	gini_coeff_of_slr_inverses
	gini_coeff_of_slrs
	human_readable_metrics
	jain_fairness_of_slr_inverses
	jain_fairness_of_slrs
	job_finish_counter
	max_job_count
	maximum_in_flight_over_cp
	maximum_slr
	mean_speedup
	mean_stretch
	mean_in_flight_over_cp
	mean_slr
	minimum_in_flight_over_cp
	minimum_slr
	network_delay_level_power
	networking_enabled
	output_schedule_to_file
	peak_in_flight
	platform_filename
	print_global_metrics
	print_metrics_by_job
	proportion_of_kind2
	quartile_slr_means
	quartile_slr_stddevs
	router_allocator
	router_orderer
	schedule_output_filename
	simulation_max_time
	staggered_release
	stddeb_in_flight_over_cp
	stddev_speedup
	stddev_stretch
	stddev_slr
	sum_of_excess_slrs
	target_utilisation
	use_supplied_submit_times
	visualisation_enabled
	visualise_each_task
	workload_makespan
	workload_filename
	worst_case_slr


    CJOBID          = UInt32Col()   # 32-bit Unsigned integer
    USER            = StringCol(64) # 64-character String
    STATUS          = StringCol(64) # 64-character String
    CLUSTER         = StringCol(64) # 64-character String
    QUEUE           = StringCol(64) # 64-character String
    MASTER_NODE     = StringCol(64) # 64-character String
    LSF_GROUP       = StringCol(64) # 64-character String
    APP             = StringCol(64) # 64-character String
    PROJECT         = StringCol(256)# 256-character String
    JOBNAME         = StringCol(256)# 256-character String
    ERROR_CODE      = StringCol(64) # 64-character String
    SUBMIT_DATETIME = UInt64Col()   # 64-bit Unsigned integer
    START_DATETIME  = UInt64Col()   # 64-bit Unsigned integer
    FINISH_DATETIME = UInt64Col()   # 64-bit Unsigned integer
    EXECUTION_TIME  = UInt64Col()   # 64-bit Unsigned integer
    RESPONSE_TIME   = UInt64Col()   # 64-bit Unsigned integer
    PENDING_TIME    = UInt64Col()   # 64-bit Unsigned integer
    CORE_SECONDS    = UInt64Col()   # 64-bit Unsigned integer
    CORES           = UInt32Col()   # 32-bit Unsigned integer




class betterdata(object):
	scheduler_bits_to_id = {

						
						}





def extract_and_pickle_data_from_folder(folder_analysis_path, output_pickle_file_path)

	pickle_file = open(
	
	
	
	
	open( pickle_filepath, "wb" )

	output_pickle_file

























def extract_even_newer():
	out_file = open(analysis_aggregate_file_path, 'w')
	
	files_to_analyse = [f for f in os.listdir(run_log_folder) if f[0] != '.']
	
	for f in files_to_analyse:
		fpath = run_log_folder + f
		res_file = open(fpath, 'r')
		for line in res_file:
			if line[0:16] == "#Results_Output#":
				out_file.write(fpath + ":" + line)
		res_file.close()
	
	out_file.close()




def analysis_even_newer():


	short_rd_pickle_filename = "short_net_pickled.pik"

	use_pickled = False
	use_short = False
	
	longlists = False
	
	if use_pickled:
		if not use_short:
			results_dicts = pickle.load( open( pickle_filepath, "rb" ) )
		else:
			results_dicts = pickle.load(open(analysis_folder + short_rd_pickle_filename, "rb"))
	else:


		
		res_file_path = analysis_aggregate_file_path
		res_file = open(analysis_aggregate_file_path, 'r')
		
		results_dicts = []
		print "reading files in"
		for line in res_file:
			entries = line.split('|')

			#types = set([e.split(':')[2] for e in entries])
			
			new_res_dict = {}
			for e in entries:
				e_parts = e.split(":")
				
				value = e_parts[1]
				
				incl = True
				
				if e_parts[2] == "int":
					value = int(value)
				elif e_parts[2] == "numpy.float64":
					value = float(value)
				elif e_parts[2] == "float":
					value = float(value)
				elif e_parts[2] == "long":
					value = long(value)
				elif e_parts[2] == "list":
					value = eval(value)
					if not longlists:
						if len(value) > 20:
							incl = False
				if incl:
					new_res_dict[e_parts[0]] = value
			results_dicts.append(new_res_dict)
		
		pickle.dump( results_dicts, open( pickle_filepath, "wb" ) )
	
	print "extracting workload names"
	
	#bit of a hack to extract workload names
	workload_kinds_list = []




if __name__ == "__main__":

	input_folder = ""
	analysis_folder = ""

	print "running extraction"

"""

