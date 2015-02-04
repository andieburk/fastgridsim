
import re
import os

main_parameter_text = [
	'"platform_filename":"platform2.plat"',
	'"debugging_on" : False',
	'"staggered_release":True',
	'"simulation_max_time":100000000',
	'"faster_less_accurate_charting": True',
	'"faster_charting_factor":100',
	'"visualisation_enabled": False',
	'"visualise_each_task" : True',
	'"fairshare_tree_filename" : "fairshares2.txt"',
	'"print_global_metrics": True']





workloads_dir = "/Users/andy/Documents/Work/grid_sim_run_files/Workload/"
orderers = ["fifo_task", "fifo_job", "fair_share"]
allocators = ["first_free"]
utilisations = ["80", "100", "120"]

params_dir = "/Users/andy/Documents/Work/grid_sim_run_files/Parameters"


workloads_raw = os.listdir(workloads_dir)
workloads = [w for w in workloads_raw if not re.match("\.", w)]

print workloads

for w in workloads:
	for u in utilisations:
		for o in orderers:
			for a in allocators:
				utxt = '"target_utilisation": "'+ u +'"'
				
				atxt = '"cluster_allocator" : "'+ a +'"'
				atxt2 = '"router_allocator" : "'+ a +'"'
				
				otxt   = '"cluster_orderer" : "'+ o +'"'
				otxt2 = '"router_orderer" : "identity"'
				wtxt = '"workload_filename" : "'+ workloads_dir + w +'"'
				
				
				output_items = main_parameter_text + [utxt, atxt, atxt2, otxt, otxt2, wtxt]
				
				output_text = '\n'.join(output_items)
				
				outfilename = params_dir + "/" + u + "-" + o + "-" + a + "-" + w
				
				print outfilename
				
				outf = open(outfilename, 'w')
				outf.write(output_text)
				outf.close()
				