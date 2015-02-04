import numpy

original_schedule_filename = "output_schedule_real_workload_whole_platform_sched.csv"
schedule_to_compare_filename = "sched_whole_fifo_job_first_free_backup_paranoia.csv"

origf = open(original_schedule_filename, "r")
compf = open(schedule_to_compare_filename, "r")


origflines = origf.readlines()[1:]
compflines = compf.readlines()[1:]

origlines_split = [l.split(',') for l in origflines]
complines_split = [l.split(',') for l in compflines]




orig_slrs = numpy.array([numpy.float32(l[4]) for l in origflines])
comp_slrs = numpy.array([numpy.float32(l[4]) for l in compflines])


small_orig_slrs =  numpy.array([numpy.float32(l[4]) for l in origflines if l[])
small_comp_slrs = numpy.array([numpy.float32((l.split(',')[4])) for l in compflines])

medium




"""
less_than2 = len([sid for sid in range(len(orig_slrs)) if comp_slrs[sid] < orig_slrs[sid]])
same_as2 = len([sid for sid in range(len(orig_slrs)) if comp_slrs[sid] == orig_slrs[sid]])
greater_than2 = len([sid for sid in range(len(orig_slrs)) if comp_slrs[sid] > orig_slrs[sid]])
"""







def stats_for(orig_slrs, comp_slrs, txt):
	print txt
		
	print len(orig_slrs)
	print len(comp_slrs)

	slr_diffs = numpy.subtract(orig_slrs, comp_slrs)

	slr_decimals = numpy.divide(comp_slrs, orig_slrs)

	total_count = len(slr_decimals)

	less_than_count = len([s for s in slr_decimals if s < 1.0])
	same_as_count = len([s for s in slr_decimals if s == 1.0])
	greater_than_count = len([s for s in slr_decimals if s > 1.0])
	lots_greater_than_count = len([s for s in slr_decimals if s > 3.0])


	within_equivalent_range_count = len([s for s in slr_decimals if (s >= 0.5) and (s <= 2.0)])
	really_shorter = len([s for s in slr_decimals if (s < 0.5)])
	really_longer = len([s for s in slr_decimals if (s > 2.0 )])




	slr_percents = slr_decimals * 100.0




	print slr_diffs[0:20]
	print slr_percents[0:20]

	print numpy.sum(slr_diffs)
	print numpy.average(slr_decimals)
	print numpy.median(slr_decimals)

	print "----------- using second (original) metrics ---------------"

	print (float(less_than_count)/ float(total_count)) * 100, "% took shorter"
	print (float(same_as_count)/ float(total_count)) * 100, "% took the same time"
	print (float(greater_than_count)/ float(total_count)) * 100, "% took longer"
	print (float(lots_greater_than_count)/ float(total_count)) * 100, "% took longer than double original time"

	print (float(within_equivalent_range_count)/ float(total_count)) * 100, "% were between -50% and +100 %"
	print (float(really_shorter)/ float(total_count)) * 100, "% were less than -50 %"
	print (float(really_longer)/ float(total_count)) * 100, "% were greater than +100 %"







