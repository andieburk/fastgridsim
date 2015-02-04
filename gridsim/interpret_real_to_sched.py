

input_real_filename = "/Volumes/NO NAME/anonymised simulation inputs/whole/anon_workload_whole.txt"

output_schedule_filename = "output_schedule_real_workload_whole_platform_sched.csv"


inputfile = open(input_real_filename, "r")
schedulefile = open(output_schedule_filename, "w")

schedulefile.write("JobName,SubmitTime,StartTime,FinishTime,SLR,CriticalPathTime,TotalCPUTicks,Stretch,Speedup,\n")

lines = inputfile.readlines()

print len(lines)

for lineid in range(len(lines)):
	if lines[lineid][0:3] == "Job":
		jbits = lines[lineid].split(" ")
		tbits = lines[lineid + 1].split(" ")
		name = jbits[1]
		submit_time = jbits[2]
		start_time = tbits[5]
		finish_time = tbits[6]
		
		slr = float(int(finish_time) - int(submit_time)) / float(int(finish_time) - int(start_time))
		
		slrstr = str(slr)
		
		out_bits = []
				
		out_bits.append(name)
		out_bits.append(submit_time)
		out_bits.append(start_time)
		out_bits.append(finish_time)
		out_bits.append(slrstr)

		out_str = ','.join(out_bits)
		schedulefile.write(out_str + "\n")

inputfile.close()
schedulefile.close()


