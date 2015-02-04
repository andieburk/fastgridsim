import datetime
import time
import cPickle as pickle
import math
import numpy
from multiprocessing import Pool

import random

class ArrivalException(Exception):
	def __init__(self, value):
		self.value = value
	def __str__(self):
		return repr(self.value)


class jsc2(object):
	def __init__(self, platform_cores, desired_utilisation, day_distribution_file = None, week_distribution_file = None):
		self.cores = platform_cores
		self.utilisation = desired_utilisation
		
		print '\t\tinitialising submit time calculator', self.cores, 'cores', self.utilisation, '% load'
		
		self.load = desired_utilisation / 100.0
		
		self.base_time = datetime.datetime(2012,1,1,0,0)
		self.next_submit_time = self.base_time
		
		if day_distribution_file == None:
			input_filename = 'day_distribution_pickled.txt'
		else:
			input_filename = day_distribution_file
		
		if week_distribution_file == None:
			input_filename_w = 'week_distribution_pickled.txt'
		else:
			input_filename_w = week_distribution_file
		
		#load day distribution
		f = open(input_filename, 'r')
		params = pickle.load(f)
		f.close()
		self.day_params = params[::-1]
		p = self.day_params
		self.hourfunc = lambda x : sum([p[i] * (x ** i) for i in range(len(p))])
		
		#load week distribution
		f = open(input_filename_w, 'r')
		params = pickle.load(f)
		f.close()
		self.week_params = params
		

		#init the minutes allocated by day distribution
		self.core_mins_day = self.core_mins_all()
		
		self.core_acc = 0
		self.week_pointer = (0, 0)
	
		self.last_submit = -1



	
	
	def core_mins_all(self):
		core_mins_week_total = 60*24*7
		hour_samples = 4
		
		hour_arr = [self.hourfunc(float(j)/hour_samples) for j in range(24*hour_samples)]
		hr_n = [h / sum(hour_arr) for h in hour_arr]
		
		core_mins_by_day_arr = [core_mins_week_total * self.week_params[i] * hr_n[j] for i in range(7) for j in range(24*hour_samples)]
		
		self.prop_mult = core_mins_week_total / (7*24*hour_samples)
		
		return core_mins_by_day_arr
	
	


	def next_activation_time_with_day_week_cycles(self, job, this_job_submit_time, exec_in_seconds = False):
	
		if this_job_submit_time < self.last_submit:
			raise ArrivalException(("submit time is less than last submit. implementation doesn't support this.", this_job_submit_time, self.last_submit))
		else:
			self.last_submit = this_job_submit_time
		
		j_exec = job.sum_of_task_exectimes()
		time_increase_basic = float(j_exec) / (float(self.load) * float(self.cores))
		time_increase_wk = self.time_increase_week_distribution(time_increase_basic)
		
		if (time_increase_wk / time_increase_basic) > (max(self.core_mins_day) / min(self.core_mins_day)):
			raise ArrivalException("time increase is ludicrously large, check the selected distributions")
		
		time_inc_week_int = int(round(time_increase_wk))
		newtime = this_job_submit_time + time_inc_week_int
		
		if newtime < this_job_submit_time:
			raise ArrivalException(("can't have a new time smaller than the old", newtime, this_job_submit_time))
		
		return newtime
		
		

	def time_increase_week_distribution(self, new_minutes):
		"""
		variable names assume each time chunk is a 'day'
		in fact, these chunks can be arbitrary sized
			
		say you have a distribution, most work arrives weekdays, less weekends
		
		each day would need, say, 60*24 * distrib_factor minutes of work submitted
		distrib_factor is >1 for weekdays, <1 for weekends.
			
		but sum([60*24*distrib_factor for distrib_factor in factors]) will = 60*24*7
			
		
		allocate a given number of minutes to each day
		two different scales - the number of real minutes to sumit
		compared to the number of minutes that will actually be submitted that day.
		
		effectively tiling up a bar chart to fill it.
		
		each bar is different heights
			
		but the time points are normalised to as if each bar was the same height (24 hours)
			
		so there is more time between submission on quiet days, and less on busy ones.
			
			
		"""
		
		curr_day = self.week_pointer[0]
		curr_pointer = self.week_pointer[1]
		day_c = 0
		real_inc_acc = 0
		big = new_minutes + curr_pointer
		
		if big < self.core_mins_day[curr_day]:
			
			proportionate_increase = new_minutes / self.core_mins_day[curr_day]
			real_increase = self.prop_mult * proportionate_increase
			
			self.week_pointer = (curr_day, big)
			
			return real_increase
			
		else:
			while big >= self.core_mins_day[curr_day]:
				p = self.core_mins_day[curr_day] - curr_pointer
				excess = big - self.core_mins_day[curr_day]
				proportionate_increase = p / self.core_mins_day[curr_day]
				real_increase = self.prop_mult * proportionate_increase
				real_inc_acc += real_increase
				
				curr_pointer = 0
				big = excess
				curr_day = (curr_day + 1) % len(self.core_mins_day)#7
		
			
			proportionate_increase = big / self.core_mins_day[curr_day]
			real_increase = self.prop_mult * proportionate_increase
			real_inc_acc += real_increase
		
			self.week_pointer = (curr_day, big)
			
			return real_inc_acc


	def next_activation_time_constant_utilisation(self, job, this_job_submit_time):
		
		if this_job_submit_time < self.last_submit:
			raise ArrivalException(("submit time is less than last submit. implementation doesn't support this.", this_job_submit_time, self.last_submit))
		else:
			self.last_submit = this_job_submit_time
	
		
		j_exec = job.sum_of_task_exectimes(cores = True)
		
		time_increase = int(math.ceil(j_exec / (self.cores * self.load)))
		
		next_time = this_job_submit_time + time_increase
		
		
		if next_time < this_job_submit_time:
			raise ArrivalException(("can't have a new time smaller than the old", next_time, this_job_submit_time))
		
		return next_time






































"""
	def core_mins_by_day(self):
		core_mins_week_total = 60*24*7#*self.cores
		
		core_mins_by_day_arr = [core_mins_week_total * self.week_params[i] for i in range(7)]
		
		return core_mins_by_day_arr
"""



		
		
"""


def js_vector(joblist, cores, load, g):

	util = float(load) / 100.0

	#g = job_submit_time_calculator(cores, load)

	job_execs = numpy.array([j.sum_of_task_exectimes() for j in joblist])

	load_core_fraction = 1.0 / (float(cores) * float(util))

	j_execs_scaled_load_cores = job_execs * load_core_fraction

	#jstarts_b = numpy.cumsum(j_execs_scaled_load_cores)
	
	#delta_starts = [g.base_time + datetime.timedelta(minutes = t) for t in jstarts_b]
	
	j_starts_c = []
	
	
	
	curr_time = g.base_time
	
	for t in j_execs_scaled_load_cores:
		
		frac = 1.0 / g.pmf_for_day(curr_time)
		frac2 = 1.0  / g.pmf_for_hour(curr_time)
		
		newt = t * frac * frac2
		
		curr_time = curr_time + datetime.timedelta(minutes = newt)
		
		j_starts_c.append(newt)
	
	je_scaled_all = numpy.array(j_starts_c)
				
	jstarts = numpy.cumsum(je_scaled_all)

	jstarts_int = numpy.round(jstarts).astype(int)


	return jstarts_int












class job_submit_time_calculator(object):
	def __init__(self, platform_cores, desired_utilisation, day_distribution_file = None, week_distribution_file = None):
		self.cores = platform_cores
		self.utilisation = desired_utilisation
		
		print '\t\tinitialising submit time calculator', self.cores, 'cores', self.utilisation, '% load'
		
		self.load = desired_utilisation / 100.0
		
		self.base_time = datetime.datetime(2012,1,1,0,0)
		self.next_submit_time = self.base_time
		
		if day_distribution_file == None:
			input_filename = 'day_distribution_pickled.txt'
		else:
			input_filename = day_distribution_file
		
		if week_distribution_file == None:
			input_filename_w = 'week_distribution_pickled.txt'
		else:
			input_filename_w = week_distribution_file
		
		
		f = open(input_filename, 'r')
		params = pickle.load(f)
		f.close()
		self.day_params = params[::-1]
	
				
		p = self.day_params
		self.hourfunc = lambda x : sum([p[i] * (x ** i) for i in range(len(p))])
	
		f = open(input_filename_w, 'r')
		params = pickle.load(f)
		f.close()
		self.week_params = params
	
		self.increase_accumulator = 0
		self.sub_acc = 0

		#diffs = []

	
	
	def pmf_for_hour(self, input_datetime):
	
		hourt = input_datetime.hour
		mint  = input_datetime.minute
		
		dayfrac = hourt+(mint/60.0)
		
		pmf = self.hourfunc(dayfrac)
		
		relative_freq = pmf * 24 * 4
		
		return relative_freq

	
	def pmf_for_day(self, input_datetime):
	
		dayt = input_datetime.weekday()
		pmf = self.week_params[dayt]
		
		relative_freq = pmf * 7
		
		return relative_freq
	
	
	
	
	def next_activation_time_with_day_week_cycles(self, job, this_job_submit_time, exec_in_seconds = False):
		if exec_in_seconds:
			sub_dt = self.base_time + datetime.timedelta(seconds = (this_job_submit_time))
		else:
			sub_dt = self.base_time + datetime.timedelta(minutes = (this_job_submit_time))
		freq_h = self.pmf_for_hour(sub_dt)
		freq_d = self.pmf_for_day(sub_dt)
		j_exec = job.sum_of_task_exectimes()
		time_increase = float(j_exec) / (float(self.cores) * self.load * freq_h * freq_d)
		self.increase_accumulator += time_increase
		if self.increase_accumulator >= 1:
			time_inc_int = int(math.floor(self.increase_accumulator))
			self.increase_accumulator -= time_inc_int	
		else:
			time_inc_int = 0	
		newtime = this_job_submit_time + time_inc_int
		return newtime
		
		
		
	def ne_dw_2(self, job, this_job_submit_time, exec_in_seconds = False):
	
	
	
		this_job_submit_time = self.sub_acc
	
	
		j_exec = job.sum_of_task_exectimes()	
	
	
		time_increase = float(j_exec) / (float(self.cores) * self.load)
		
		
	
	
		if exec_in_seconds:
			sub_dt = self.base_time + datetime.timedelta(seconds = (this_job_submit_time + (time_increase/2.0)))
		else:
			sub_dt = self.base_time + datetime.timedelta(minutes = (this_job_submit_time + (time_increase/2.0)))
			
			
		#print sub_dt
		
		#exit()
			
			
		freq_h = self.pmf_for_hour(sub_dt)
		freq_d = self.pmf_for_day(sub_dt)
		
		ti2 = time_increase * (1.0 / (freq_d * freq_h))

		
		self.increase_accumulator += ti2
		
		
		
		if self.increase_accumulator >= 1:
			time_inc_int = int(math.floor(self.increase_accumulator))
			self.increase_accumulator -= time_inc_int
		
		
		else:
			time_inc_int = 0
		
		newtime = this_job_submit_time + time_inc_int
		


		#print this_job_submit_time, time_inc_int, ti2, newtime

		self.sub_acc += ti2

		newtime_t = self.base_time + datetime.timedelta(minutes = newtime)
		sub_acc_t = self.base_time + datetime.timedelta(minutes = self.sub_acc)


		#print 'drift', sub_acc_t - newtime_t

		

		return newtime

	
	def next_activation_time_constant_utilisation(self, job, this_job_submit_time):
		
		j_exec = job.sum_of_task_exectimes(cores = True)
		
		time_increase = int(math.ceil(j_exec / (self.cores * self.load)))
		
		next_time = this_job_submit_time + time_increase
		
		
		if next_time < this_job_submit_time:
			raise ArrivalException(("can't have a new time smaller than the old", next_time, this_job_submit_time))
		
		return next_time



def inner_bit(load, joblist, num_procs, g):

	newtimes = js_vector(joblist, num_procs, load, g)
	workload_volume = sum([j.sum_of_task_exectimes() for j in joblist])
	final_time = newtimes[-1]
	
	total_possible_time = num_procs * final_time
	
	
	used_fraction = 100*(float(workload_volume) / (total_possible_time))
	
	#print 'u', load, 'workload volume', workload_volume, '/ total possible time', total_possible_time, 'f', used_fraction, 'days', final_time / (60*24.0)
	
	return abs(10.0 - used_fraction)







def bob2(load):



	from workload_generator_3 import log_linear_industrial_raw	
	from scipy.optimize import fmin
	
	
	
	num_procs = 1000
	newtimes = []
	
	ts = log_linear_industrial_raw(20000)
	#ts = [500 for x in range(100000)]
	
	joblist = [job(t) for t in ts]
	
	g = job_submit_time_calculator(num_procs, load)	
	
	val1 = numpy.array([11.0])
	res = fmin(inner_bit,val1, ftol=0.1)
	
	print res
	





	



	



	
	





def bob(load):

	from workload_generator_3 import log_linear_industrial_raw


	ts = log_linear_industrial_raw(40000)
	num_procs = 1000
	

	#load = (i+1)*10
	
	g = jsc2(num_procs, load)
	
	joblist = [job(t) for t in ts]
	
	
	newtimes = []
	
	curr_time = 0
	curr_exec_total = 0
	
	
	fracts = []
	
	
	for j in joblist:
		nt = g.next_activation_time_with_day_week_cycles(j, curr_time, exec_in_seconds = False)
		curr_exec_total += j.sum_of_task_exectimes()
		
		curr_time = nt
		
		newtimes.append(nt)
		if curr_time > 0:
		
			fract = 100.0 * ( float(curr_exec_total) / float(num_procs * curr_time))
		
			#curr_time = curr_time * (fract/(load/100.0))
		
		
		
		
		
		
			fracts.append(fract)
	
	plt.plot(fracts)
	plt.show()
	
	#exit()
	
	
	
	workload_volume = sum([j.sum_of_task_exectimes() for j in joblist])

	final_time = newtimes[-1]
	
	print workload_volume, curr_exec_total
	print curr_time, final_time

	
	total_possible_time = num_procs * final_time
	
			
	used_fraction = 100*(float(workload_volume) / (total_possible_time))
	
	print 'u', load, 'workload volume', workload_volume, '/ total possible time', total_possible_time, 'f', used_fraction, 'days', final_time / (60*24.0)
	#print 'time needed at 100%', workload_volume / float(num_procs)
	

	
	exit()
	
	tg = [g.base_time + datetime.timedelta(minutes = t) for t in newtimes]
	
	days = [t.weekday() for t in tg]
	
	print len(days)
	print len(ts)
	

	x = numpy.bincount(days)#, weights = ts)
	z = numpy.bincount(days, weights = ts)
	print x
	y = [i/float(sum(x)) for i in x]
	p = [i/float(sum(z)) for i in z]
	print y
	print g.week_params
	
	plt.plot(y)
	plt.plot(p)
	plt.plot(g.week_params)
	plt.show()
	
	
	
	
	#print newtimes[:100]
	#print newtimes[-1]
	
	

	return load, used_fraction






class job(object):
	def __init__(self, exectime):
		self.exectime = exectime
	def sum_of_task_exectimes(self):
		return self.exectime


def xfive(x):
	return x*5.0

def diffx(val):
	return abs(10 - xfive(val[0]))



if __name__ == "__main__":
	from workload_generator_3 import log_linear_industrial_raw
	import matplotlib.pyplot as plt


	#h = jsc2(1000, 100)




	#exit()


	bob(100)



	exit()


	print "go"




	
		
		
				
	



	
	load = 200
	
	
	bob2(20)
	
	exit()
	
	
	
	interesting_loads = [(i+1) for i in range(200)]
	
	p = Pool(8)
	
	fracs = p.map(bob, interesting_loads)
	
	print fracs
	
	
	
	#plt.plot([f[0] for f in fracs])
	#plt.plot([f[1] for f in fracs])
	plt.plot(sorted([f[1] - f[0] for f in fracs]))
	plt.show()
	
	exit()

	

	
	exit()
	
	
	
	fracs = []

	
	
	for i in range(20):
		load = (i+1)*10
		
		g = job_submit_time_calculator(num_procs, load)
		
		joblist = [job(random.randrange(100,100000)) for j in range(100000)]
		

		newtimes = []
		
		curr_time = 0
		
		for j in joblist:
			nt = g.next_activation_time_with_day_week_cycles(j, curr_time)
			curr_time = nt
			
			newtimes.append(nt)



		
		workload_volume = sum([j.sum_of_task_exectimes() for j in joblist])
		final_time = newtimes[-1]
		
		total_possible_time = num_procs * final_time
		
		#print 'workload volume', workload_volume
		#print 'total possible time', total_possible_time
		#print 'time needed at 100%', workload_volume / float(num_procs)
		
		
		used_fraction = 100*(float(workload_volume) / (total_possible_time))
		
		#print newtimes[:100]
		#print newtimes[-1]
		
		
		fracs.append(load - used_fraction)
		
	
	
	print fracs
	
		
	import matplotlib.pyplot as plt


	plt.plot(fracs)
	plt.show()

	exit()



	
	#newtimes = [g.base_time + datetime.timedelta(minutes = g.next_activation_time_with_day_week_cycles(joblist[0], i)) for i in range(10000)]

#	new_xs = [g.base_time + datetime.timedelta(minutes = n) for n in newtimes]
#jobtimes = new_xs
	
	#num_procs = 1



	print 'used fraction', used_fraction
	
	#if used_fraction >
	
	
	
	
	
	g = job_submit_time_calculator(1000, 100)
	
	hours = [g.base_time + datetime.timedelta(minutes = i) for i in range(24*60)]
	
	
	hourdist = [g.pmf_for_hour(h) for h in hours]
	
	
	
	print 'hourdistsum', sum(hourdist), sum(hourdist) / float(24*60*100)
	
	
	plt.plot(hourdist)
	plt.show()
	exit()


	th = numpy.array([(n.weekday() * 24 ) + n.hour for n in new_xs])

	x = numpy.bincount(th)


	plt.bar(range(len(x)), x )
	plt.show()


	#tm = [g.base_time + datetime.timedelta(minutes = m) for m in range(7*24*60)]

	#ts = numpy.array([g.pmf_for_hour(t)*24*4 for t in tm])

	#tw = numpy.array([g.pmf_for_day(t)*7 for t in tm])

	#tc = ts * tw
	


	#ts = [g.actual_pmf_hour(i/20.0) for i in range(24*20)]
	
	#ty = 24 * 4 * numpy.array(ts)
	
	#tz = 1.0 / ty

	#print sum(ts)
	#print ts







"""

















































