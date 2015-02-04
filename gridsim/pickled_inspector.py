import cPickle as pickle

pickfile = "/Users/andy/Desktop/picktest/pick_runlog_for_example_simulation_parameters1.py.bin"


with open(pickfile, "rb") as pick_file_handle:
	results_dict = pickle.load(pick_file_handle)


for i in sorted(results_dict.items(), key=lambda x : x[0]):
	print i[0], ':',  i[1]


