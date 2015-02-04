from pool_runner import *
import os

parameters_folder_path = "/Users/andy/Documents/Work/airbus_workload_new_params/"
output_logs_path = "/Users/andy/Documents/Work/airbus_workload_new_runlogs/"

parameters = os.listdir(parameters_folder_path)

base_exec_string = "python gridsim.py "
#base_exec_string = "/Library/Frameworks/EPD64.framework/Versions/7.0/bin/python gridsim.py " 
#base_exec_string = "/Users/andy/Downloads/pypy-c-jit-54752-6dffe8f51e7b-osx64/bin/pypy gridsim.py "
end_exec_string = " > "

exec_strings = [base_exec_string + parameters_folder_path + p + " > " + output_logs_path + "run_log_for_" + p for p in parameters
                if (p[0] != '.') and (not os.path.exists(output_logs_path + "run_log_for_" + p))]


actual_exec_strings = exec_strings

print len(actual_exec_strings), "simulations to run"

parallel_exec_strings(actual_exec_strings)

print len(actual_exec_strings), "simulations finished executing"