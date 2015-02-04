
import tables
from text_log_to_pytable import text_to_csv_to_h5_transform_manager
import collections
from datetime import datetime


"""
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




"""

params = {}

params["input_file"] = "/Volumes/NO NAME/jobs_bits_processing/jobs4.txt"
params["intermediate_csv_file"] = "/Volumes/NO NAME/jobs_bits_processing/jobs4csv.csv"
params["h5_file"] = "/Volumes/NO NAME/jobs_bits_processing/jobs4_h5.h5"
params["quiet"] = True



#transformer = text_to_csv_to_h5_transform_manager()
#transformer.transform(params["input_file"],
#					  params['intermediate_csv_file'],
#					  params['h5_file'],
#					  not params["quiet"])




data_array = tables.openFile(params['h5_file'])

all_jobs_bits = collections.defaultdict(lambda : [])

cluster_counts = collections.defaultdict(lambda : 0)
cluster_starts = collections.defaultdict(lambda : 999999999999999999999999)


start_time = 1315180800 #start time of when all the clusters present (after midnight on the first monday afterwards. this happens to be midnight on Monday 5th Sep 2011)
overall_start = str(start_time)



extract_string = "(FINISH_DATETIME > 0) & (START_DATETIME < FINISH_DATETIME) & (CLUSTER != 'fil_a') & (SUBMIT_DATETIME >= 1315180800)"

transitions = collections.defaultdict(lambda : 0)
#tou1_trans = collections.defaultdict(lambda : 0)
#tou2_trans = collections.defaultdict(lambda : 0)



def peak_cores_from_transitions(cluster, transitions):
	peak = 0
	current = 0
	
	all = []
	
	
	sorted_transitions = sorted(transitions.items(), key= lambda x: x[0][1])
	
	
	for t in sorted_transitions:
		if t[0][0] == cluster:
			current += t[1]
			if current > peak:
				peak = current
			all.append(current)

	toptwenty = sorted(all)[-20:]

	return peak, toptwenty

jnamecounter = 0
tnamecounter = 0


all_tasks_by_user_submit = collections.defaultdict(lambda : [])

for row in data_array.root.jobs.extract.where(extract_string):
	
	
	tnamecounter += 1
	
	fairshare_path = '/'+ row["LSF_GROUP"] + '/' + row["USER"]
	sub_time = row["SUBMIT_DATETIME"]
	
	task = {}
	
	task["TASK_NAME"] = 't' + str(tnamecounter)
	task["SUBMIT_DATETIME"] = row["SUBMIT_DATETIME"]
	task["EXECUTION_TIME"] = row["EXECUTION_TIME"]
	task["CORES"] = row["CORES"]
	task["FAIRSHARE_PATH"] = fairshare_path
	
	
	all_tasks_by_user_submit[(fairshare_path, sub_time)].append(task)
	
	
	"""	row_dict = {"SUBMIT_DATETIME" : row["SUBMIT_DATETIME"],
				"START_DATETIME" : row["START_DATETIME"],
				"FINISH_DATETIME" : row["FINISH_DATETIME"],
				"SUBMIT_DATETIME" : row["SUBMIT_DATETIME"],
				"SUBMIT_DATETIME" : row["SUBMIT_DATETIME"],
				"SUBMIT_DATETIME" : row["SUBMIT_DATETIME"],
				"SUBMIT_DATETIME" : row["SUBMIT_DATETIME"],
				"SUBMIT_DATETIME" : row["SUBMIT_DATETIME"],
				
	
				}
	
	
	
	
	

	print row["SUBMIT_DATETIME"]
	print row["START_DATETIME"]
	print row["FINISH_DATETIME"]
	print row["USER"]
	print row["LSF_GROUP"]
	print row["PROJECT"]
	print row["CORES"]
	print row["EXECUTION_TIME"]
	print row["JOBNAME"]
	print row["CLUSTER"]
	"""
	
	transitions[(row["CLUSTER"], row["START_DATETIME"])] += row["CORES"]
	transitions[(row["CLUSTER"], row["FINISH_DATETIME"])] -= row["CORES"]
	
	
	
	
	cluster_counts[row["CLUSTER"]] += 1
	if cluster_starts[row["CLUSTER"]] > row["SUBMIT_DATETIME"]:
		cluster_starts[row["CLUSTER"]] = row["SUBMIT_DATETIME"]



#print cluster_counts

#for i in cluster_starts.items():
#	print i[0], datetime.fromtimestamp(i[1])

#print 'peak ham_a', peak_cores_from_transitions('ham_a', transitions)
#print 'peak tou_b', peak_cores_from_transitions('tou_b', transitions)
#print 'peak tou_c', peak_cores_from_transitions('tou_c', transitions)

jobs_with_more_than_1_task = 0

tnamecounter2 = 0

for j in sorted(all_tasks_by_user_submit.values(), key= lambda x : x[0]["SUBMIT_DATETIME"]):
	if len(j) > 1:
		jobs_with_more_than_1_task += 1

	jnamecounter += 1
	print "\nJob j" + str(jnamecounter) + " " + str(j[0]["SUBMIT_DATETIME"]) + " " + j[0]["FAIRSHARE_PATH"]
	for t in j:
		tnamecounter2 += 1
		print "Task t" + str(tnamecounter2) + ' ' + str(t["CORES"]) + ' ' + str(t["EXECUTION_TIME"]) + ' Kind1 '

#print jobs_with_more_than_1_task, float(jobs_with_more_than_1_task)/ float(len(all_tasks_by_user_submit))







exit()







"""


        first_timestamp_column_string  = py_exps.START_DATETIME
        second_timestamp_column_string = py_exps.FINISH_DATETIME

    else:
        evaluate_string = py_exps.exp_in_range('pend', range_start, range_finish)
        first_timestamp_column_string  = py_exps.SUBMIT_DATETIME
        second_timestamp_column_string = py_exps.START_DATETIME    
    
    #data_array.root.jobs.extract.where is the submission of the pytables query
    
    all_jobs_in_range = [[row[first_timestamp_column_string],
                          row[second_timestamp_column_string],
                          row['CORES'],
                          row['PROJECT'],
                          row['LSF_GROUP'],
                          row['APP']] 
                        for row in data_array.root.jobs.extract.where(evaluate_string)]
        
        


    def open_file(self, filename):
        ''' wrapper for the pytables open file command'''
        self.data_array = tables.openFile(filename)

data_array.root.jobs.extract.where(evaluate_string)]






"""