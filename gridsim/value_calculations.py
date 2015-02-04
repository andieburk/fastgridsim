from task_state_new import TaskState
#read in curve definition file

import scipy.integrate
from scipy.interpolate import interp1d
import copy
import numpy
import collections
#import numexpr

class ValueException(Exception):
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return repr(self.value)


def value_curve_valid(value_curve):
	
	#curve is defined as a list of pairs (x,y)
	#where x = (time - j-submit)/(j-critical-path), or what the SLR of the job would be if it finished at time (so x is multiplier of critical path)
	#and y is the normalised value (between 1 and -1)
	
	for val in value_curve:
		if val[0] < 1.0: #cp-multipliers must be > 1 (job can't have meaningful changes in value before it could ever finish)
			return False
		if val[1] < -1.0: #max penalty multiplier is -1
			return False
		if val[1] > 1.0:
			return False #max value multiplier is +1
	
	#curve points should be sorted
	value_curve_sorted = sorted(value_curve, key=lambda x: x[0])
	if value_curve_sorted != value_curve:
		return False
	
	#values must be same or decreasing
	if len(value_curve) > 1:
		for id in range(len(value_curve) - 1):
			if value_curve[id+1][1] > value_curve[id][1]:
				return False
	
	#there must be at least one transition
	if len(value_curve) < 1:
		return False
	
	#the value must eventually go to 0 or less
	if min([x[1] for x in value_curve]) > 0:
		return False
	
	return True


def interp_function_from_curves_point_list(curve_point_list):
	x = [c[0] for c in curve_point_list]
	y = [c[1] for c in curve_point_list]
	f = scipy.interpolate.interp1d(x, y)

	#print f(numpy.array(range(9)) + 1)
	#print [f(x+1) for x in range(9)]
	#exit()

	return f




class value_curve(object):
	def __init__(self, name, curve_point_list):
		self.name = name
		self.curve_point_list = curve_point_list
		
		time_index = 0
		value_index = 1
		
		self.times = numpy.array([c[time_index] for c in self.curve_point_list])
		self.values  = numpy.array([c[value_index] for c in self.curve_point_list])
		
		self.min_time_point = min(self.times)
		self.max_time_point = max(self.times)
		
		self.value_at_end_of_curve = self.curve_point_list[-1][value_index]
		
		self.factor_function = scipy.interpolate.interp1d(self.times, self.values)
		
		self.last_slr = None
		self.last_value = None
		#print "value curve", name, "init", curve_point_list
	
	def factor(self, slr):
	
		if slr == self.last_slr:
			return self.last_value
		
		if slr <= self.min_time_point:
			#print 'min'
			factor = 1.0
		elif slr >= self.max_time_point:
			#print 'max'
			factor = self.value_at_end_of_curve
		else:
			#print 'func'
			factor = self.factor_function(slr)
			#print self.curve_point_list
		
		
		self.last_slr = slr
		self.last_value = factor
		
		
		#print 'factor', factor
				
		return factor


def read_curves_file(filename):
	filehandle = open(filename, 'r')
	
	curves = {}
	

	begin = True
	
	for line in filehandle:
		lsplit = line.rstrip().split(' ')
		
		if (len(lsplit) > 0) and (len(line.rstrip()) > 0):
			
			if lsplit[0].lower() == 'curve':
				if not begin:
					if value_curve_valid(curve_point_list) :
						curves[curr_curve_name] = value_curve(curr_curve_name, curve_point_list)
					else:
						print "invalid curve list", curve_point_list
						exit()
				begin = False
				curr_curve_name = lsplit[1]
				curve_point_list = []
			elif len(lsplit) == 2:
				#print lsplit[0], lsplit[1]
				curve_point_list.append((float(lsplit[0]), float(lsplit[1])))
			else:
				print 'malformed line?'
				print line, len(line)
				print lsplit, len(lsplit)

					
	if value_curve_valid(curve_point_list) :
		curves[curr_curve_name] = value_curve(curr_curve_name, curve_point_list)
	else:
		print "value curve read invalid", curve_point_list

	return curves





"""
def factor_calculator(value_curve, time):
	#deal with time values beyond the ends of the curves
	if not value_curve_valid(value_curve):
		print "fatal error, value curve", value_curve, "is invalid"
		exit()
	else:
		last_index = -1
		time_index = 0
		value_index = 1
		
		times = [x[time_index] for x in value_curve]
		min_time_point = min(times)
		max_time_point = max(times)
		
		
		if time < min_time_point:
			factor = 1.0
		elif time > max_time_point:
			factor = value_curve[last_index][value_index]
		else:
			factor = interpolate_factor_from_curve(value_curve, time)
	
	return factor


def interpolate_factor_from_curve(curve, time):
	#linear interpolation between points on the 'curve'. straight lines really.
	larger_points = sorted([c for c in curve if c[0] >= time], key=lambda x: x[0])
	next_larger_point = larger_points[0]
	smaller_points = sorted([c for c in curve if c[0] <= time], key=lambda x: x[0])
	next_smaller_point = smaller_points[-1]
	
	#print next_larger_point, next_smaller_point
	
	if next_larger_point == next_smaller_point:
		return next_larger_point[1]
	else:
		proportion_along = (time - next_smaller_point[0]) / (next_larger_point[0] - next_smaller_point[0])
		factor = next_smaller_point[1] + (proportion_along * (next_larger_point[1] - next_smaller_point[1]))
		return factor




"""

def job_penalty(job):
	jobs_cpu_ticks_used = sum([t.corecount * t.get_execution_time(imprecise_estimate = False)
							   for t in job.tasklist if t.completed])
							  # if (t.state_manager.state == TaskState.running) or (t.state_manager.state == TaskState.completed)])
	return jobs_cpu_ticks_used


def task_core_exec(t):
	return t.corecount * t.get_execution_time(imprecise_estimate = not t.completed)



def value_from_factor(factor, job):

	jobs_cpu_ticks_total = sum([t.get_core_count() * t.get_execution_time(imprecise_estimate = not t.completed) for t in job.tasklist])
	
	#the value given for the job at this point is the normalised value (or factor) * the compute time of the job
	value = factor * jobs_cpu_ticks_total
	
	
	if factor <= 0.0: #apply penalty
		#as configured here, two penalties can kick - the wasted execution time
		#as well as the negative penalty of being late
		#meaning worst penalty is 2x best value
		#jobs_cpu_ticks_used = sum([t.get_core_count() * t.get_execution_time(imprecise_estimate = False)
		#						   for t in job.tasklist
		#						   if (t.state_manager.state == TaskState.running) or (t.state_manager.state == TaskState.completed)])
		
		value = 0# value - job_penalty(job)
	
	
	if job.debugging_on:
		print "job id", job.jobid
		print "job cpu ticks not yet:", jobs_cpu_ticks_not_yet_run
		print "job cpu ticks run", jobs_cpu_ticks_run
		print "job cpu ticks total:", jobs_cpu_ticks_total
		print "factor", factor
		print "value", value
	
	
	
	return value


def potential_positive_value_remaining(value_curve, job, time, predicted_job_slr):
	
	curr_value = value_curve.factor(predicted_job_slr) #factor_calculator(value_curve, predicted_job_slr)
	
	if predicted_job_slr not in [v[0] for v in value_curve.curve_point_list]:
		new_value_curve = copy.copy(value_curve.curve_point_list)
		new_value_curve.append((predicted_job_slr, curr_value))
	else:
		new_value_curve = value_curve.curve_point_list
	
	useful_points = [x for x in new_value_curve if (x[1] >= 0) and (x[0] >= predicted_job_slr)]
	useful_points.sort(key = lambda x: x[0])
	
	if len(useful_points) < 2:
		if job.debugging_on:
			print "returning 0 for expected value because"
			print "curve given", value_curve.curve_point_list
			print "predicted slr", predicted_job_slr
			print useful_points
			
		return 0

	value_curve_xs = numpy.array([x[0] for x in useful_points])
	value_curve_ys = numpy.array([x[1] for x in useful_points])

	value_area_left = scipy.integrate.trapz(value_curve_ys, value_curve_xs)

	if job.debugging_on:
		print value_curve_xs
		print value_curve_ys
		print "value area left", value_area_left

	return value_area_left


def tasklistwalk(ls, pred_slrs, curvesdict):

	associated_curve_points = []
	associated_curve_values = []
	job_execs				= numpy.zeros(len(ls))
	
	job_cpu_ticks_dict = collections.defaultdict(lambda: -1)
	
	
	i = 0
	for t in ls:
		parent_job_name = t.parent_job.name
		job_cpu_ticks_total = job_cpu_ticks_dict[parent_job_name]
		if job_cpu_ticks_total == -1:
			job_cpu_ticks_total = sum([ti.imp_cores_fast[not t.completed][True]
									   for ti in t.parent_job.tasklist])
			job_cpu_ticks_dict[parent_job_name] = job_cpu_ticks_total
				
		job_execs[i] = job_cpu_ticks_total
		
		parent_curve = curvesdict[t.parent_job.value_curve_name]
		curve_points = parent_curve.times
		curve_values = parent_curve.values
		
		associated_curve_points.append(curve_points)
		associated_curve_values.append(curve_values)
		i += 1
		
	
	curve_points = numpy.array(associated_curve_points)
	curve_values = numpy.array(associated_curve_values)
	slrs_for_match = numpy.reshape(pred_slrs, (len(pred_slrs), 1))
	#job_execs = numpy.array(parent_job_execs)


	return curve_points, curve_values, slrs_for_match, job_execs





def index_calculations(curve_points, slrs_for_match, pred_slrs, debugging_on):
	#test the values in the curve points to find the correct index
	#this operation could be heavy - are there quicker ways? (numexpr?)
	
	mins = numpy.zeros(len(pred_slrs))
	maxs = numpy.zeros(len(pred_slrs))
	indices = numpy.zeros(len(pred_slrs))
	
	for i in xrange(len(pred_slrs)):
		less_testeds = curve_points[i] < slrs_for_match[i]
		mins[i] = not less_testeds[0]
		maxs[i] = less_testeds[-1]
		if not (mins[i] or maxs[i]):
			indices[i] = numpy.count_nonzero(less_testeds)-1

	#don't bother interpolating ones where the values are smaller or larger than the curve
	mins = mins.astype(bool)
	maxs = maxs.astype(bool)
	not_mins_or_maxs = numpy.logical_not(numpy.logical_or(mins, maxs))

	#filter out the ones you don't need
	indices_not_mins_or_maxs = indices[not_mins_or_maxs]

	if debugging_on:
		less_testeds_not_morm	 = less_testeds[not_mins_or_maxs]
		for i in range(len(indices_not_mins_or_maxs)):
			if less_testeds_not_morm[i][indices_not_mins_or_maxs[i]] == less_testeds_not_morm[i][indices_not_mins_or_maxs[i] + 1]:
				print "error:"
				print less_testeds_not_morm
				print indices_not_mins_or_maxs < 0
				print indices_not_mins_or_maxs
				raise ValueException("indices mismatching")


	return indices, indices_not_mins_or_maxs, mins, maxs, not_mins_or_maxs


def vector_interpolate(pred_slrs, indices, not_mins_or_maxs, indices_not_mins_or_maxs, curve_points, curve_values, final_factors):

	#set up the arrays for vectored interpolation
	cp_i = curve_points[not_mins_or_maxs]
	cv_i = curve_values[not_mins_or_maxs]

	interp_low	= numpy.zeros(len(indices_not_mins_or_maxs))
	interp_high = numpy.zeros(len(indices_not_mins_or_maxs))
	val_low		= numpy.zeros(len(indices_not_mins_or_maxs))
	val_high	= numpy.zeros(len(indices_not_mins_or_maxs))
	
	
	
	
	for i in xrange(len(indices_not_mins_or_maxs)):
		
		interp_low[i] = cp_i[i][indices_not_mins_or_maxs[i]]
		interp_high[i] = cp_i[i][indices_not_mins_or_maxs[i]+1]

		val_low[i] = cv_i[i][indices_not_mins_or_maxs[i]]
		val_high[i] = cv_i[i][indices_not_mins_or_maxs[i]+1]


	#do the vectored interpolation - again, could be heavy - check this later
	value = val_low + (((pred_slrs[not_mins_or_maxs] - interp_low)/(interp_high - interp_low)) * (val_high - val_low))
	
	
	final_factors[not_mins_or_maxs] = value
	
	#return value



def min_bits(mins, curve_values, final_factors, debugging_on):
	
	#tasks before the min point on their value curve (just 1 really)
	if numpy.any(mins):
		final_factors[mins] = numpy.array([c[0] for c in curve_values[mins]])
		
		if debugging_on:
			for p in final_factors[mins]:
				if p < 1.0:
					print curve_values[mins,0]
					raise ValueException("initial value too low")


def max_bits(maxs, ls,  job_execs, final_factors):
	#tasks past their max point on the value curve
	if numpy.any(maxs):
		penalties = []
		for i in xrange(len(maxs)):
			if maxs[i]:
				jobs_cpu_ticks_used = sum([t.imp_cores_fast[False][True]
										   for t in ls[i].parent_job.tasklist if t.completed])
				penalties.append(jobs_cpu_ticks_used)
		pen_array = numpy.array(penalties)
		penalty_factors = pen_array / job_execs[maxs].astype(float)
		
		final_factors[maxs] = 0# - penalty_factors #job_execs[maxs]#curve_values[maxs][-1]




def value_calculator_vectored(ls, projected_parts_dict, debugging_on = False):
	
	pred_finishes	= projected_parts_dict["predicted_finish_time_array"]
	pred_slrs		= projected_parts_dict["predicted_SLR"]
	
	curvesdict = ls[0].parent_job.parent_application_manager.curvesdict
	
	
	curve_points, curve_values, slrs_for_match, job_execs = tasklistwalk(ls, pred_slrs, curvesdict)

	indices, indices_not_mins_or_maxs, mins, maxs, not_mins_or_maxs = index_calculations(curve_points, slrs_for_match, pred_slrs, debugging_on)

	final_factors = numpy.zeros(len(pred_slrs))
	
	vector_interpolate(pred_slrs, indices, not_mins_or_maxs, indices_not_mins_or_maxs, curve_points, curve_values, final_factors)
	min_bits(mins, curve_values, final_factors, debugging_on)
	max_bits(maxs, ls,  job_execs, final_factors)

	final_values = final_factors * job_execs

			
			
			
			
			
	#print pred_slrs
	#print value
	"""		
	print "testing - remove this slow bit after proved to be right"
	
	projected_value_function = lambda id : actual_value_calculator(
													curvesdict[ls[id].parent_job.value_curve_name],
													ls[id].parent_job,
													projected_parts_dict["predicted_finish_time_array"][id],
													projected_parts_dict["predicted_SLR"][id]
													)
			
	task_projected_values = numpy.array([projected_value_function(id) for id in range(len(ls))])
	
			
			
	for id in range(len(final_values)):
		if abs(final_values[id].astype(float) - task_projected_values[id].astype(float)) > 0.000001:
			
			print 'id',id

			
			
			
			print 'final values',final_values[id], 'task projected values',task_projected_values[id]
			print 'pred slr',pred_slrs[id]
			print 'curve points', curve_points[id]
			print 'curve values', curve_values[id]
			print '--'
			
			print 'penalty', pen_array
			print 'penalty factor', penalty_factors
			
			
			print 'final factor', final_factors[id]
			
			print 'fract', fract
		
			print 'diff',diff
			
			
			
			print 'val low',val_low
			print 'val high', val_high
			
			
			print 'value', value
			
			
			print 'interp_low', interp_low
			print 'interp_high', interp_high
			
			
			print 'less testeds',less_testeds
			print 'mins',mins
			print 'maxs',maxs
			
			
			print 'indices',indices
			print '\n\n\n'
			print 'task projected values', task_projected_values
			print 'final values', final_values

			import matplotlib.pyplot as plt
			
			plt.scatter(curve_points[id], curve_values[id]*job_execs[id])
			plt.scatter([pred_slrs[id]], [final_values[id]],color='g')
			plt.scatter(pred_slrs[id], task_projected_values[id], color='r')
			plt.show()

				
			raise ValueException("mismatch in value calculations")
		else:
			pass
			#print "yay"
	"""
	

	return final_values
















def actual_value_calculator(value_curve, job, time, predicted_job_slr):
	
	
	if predicted_job_slr < 1.0:
		#can happen for tasks not on critical path during simulation (at end, they will always be >= 1.0)
		considered_slr = 1.0
	else:
		considered_slr = predicted_job_slr

	if job.debugging_on:
		print value_curve.name
		print value_curve.factor
		print considered_slr
		print value_curve.factor(predicted_job_slr)
		print value_from_factor(value_curve.factor(predicted_job_slr), job)
		print "\n\n\n"
		
	if job.timed_out and (predicted_job_slr > 1.0):
		return -job_penalty(job) #check this - may want a penalty
	
	factor = value_curve.factor(considered_slr) #factor_calculator(value_curve, predicted_job_slr)
	if job.debugging_on:
		print "value curve", value_curve.curve_point_list, "time", time
	
	value = value_from_factor(factor, job)
	return value


if __name__ == "__main__":
	curves = read_curves_file("/Users/andy/Documents/Work/gridsimxcode/GridSim/gridsim/value_curves.txt")

	print len(curves), "curves read"
	for c in curves:
		print curves




