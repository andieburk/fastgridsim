from pool_runner import *
import os


def xgrid_string(p_filename):

	input_filename = parameters_folder_path + p_filename
	output_filename = output_logs_path+ output_file_prefix + p_filename
	
	all_strings = [
		xgrid_command,
		xgrid_password,
		xgrid_job_exec,
		xgrid_run_command,
		gridsim_source_path,
		python_executable_path,
		source_python_file,
		input_filename,
		]
	
	return ' '.join(all_strings)
	
	







parameters_folder_path = "/Users/andy/Documents/Work/log_converter/parameters/"
output_logs_path = "/Users/andy/Documents/Work/log_converter/run_logs/"
python_executable_path = "/Library/Frameworks/EPD64.framework/Versions/current/bin/python"
gridsim_source_path = "/Users/andy/Documents/Work/gridsimxcode/GridSim/gridsim/"

xgrid_password = "-password secret"
xgrid_command = "xgrid"
xgrid_job_exec = "-job submit"
xgrid_run_command = "-in"
source_python_file = "gridsim.py"
xgrid_output_command = "-so"
output_file_prefix = "run_log_for_"
xgrid_output_folder_command = "-out"

parameters = os.listdir(parameters_folder_path)


use_pool = True

use_xgrid = False




if use_pool:
	base_exec_string = python_executable_path + " " + gridsim_source_path + "gridsim.py "

	#base_exec_string = "/Library/Frameworks/EPD64.framework/Versions/7.0/bin/python gridsim.py " 
	#base_exec_string = "/Users/andy/Downloads/pypy-c-jit-54752-6dffe8f51e7b-osx64/bin/pypy gridsim.py "
	end_exec_string = " > "

	exec_strings = [base_exec_string + parameters_folder_path + p + " > " + output_logs_path + "run_log_for_" + p for p in parameters
					if (p[0] != '.') and (not os.path.exists(output_logs_path + "run_log_for_" + p))]



elif use_xgrid:
	exec_strings = [xgrid_string(p) 
					for p in parameters
					if (p[0] != '.') and (not os.path.exists(output_logs_path + "run_log_for_" + p))]

	print exec_strings[0]
	print exec_strings[0:3]


else:
	print "error - need to use some runner"
	exit()



print len(exec_strings)

actual_exec_strings = exec_strings

print actual_exec_strings

print len(actual_exec_strings), "simulations to run"

parallel_exec_strings(actual_exec_strings)

print len(actual_exec_strings), "simulations finished executing"








