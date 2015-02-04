#to generate valid value curves

import random
import numpy
#import matplotlib.pyplot as plt

class InputError(Exception):
	pass






def random_curve(min_points, max_points, min_start_slr, max_start_slr, max_end_slr, max_zero_slr = None, go_negative=False):
	
	if not go_negative:
		number_of_points = random.randint(min_points, max_points)
		
		used_end_slr = max_end_slr
		if max_zero_slr != None:
			if max_zero_slr < used_end_slr:
				used_end_slr = max_zero_slr
		

		actual_end_slr = random.uniform(max_start_slr, used_end_slr)
		
					
					
		start_slr = random.uniform(min_start_slr, max_start_slr)

		if start_slr >= used_end_slr:
			raise InputError("start SLR range is >= end SLR range")
				
		time_points = numpy.random.uniform(low=start_slr, high=actual_end_slr, size=number_of_points)
		value_points = numpy.random.uniform(low=0.0, high=1.0, size=number_of_points)
		
		time_points.sort()
		value_points.sort()

		curve_points = []
		curve_points.append((1.0, 1.0))
		curve_points.append((start_slr, 1.0))
		curve_points.extend([(time_points[i], value_points[number_of_points - (i+1)]) for i in range(number_of_points)])
		
		curve_points.append((actual_end_slr, 0.0))

		return curve_points

def output_n_curves_to_file(number_of_curves, filename):

	filehandle = open(filename, 'w')

	for i in range(number_of_curves):
		curve = random_curve(3, 20, 1.2, 4.0, 10.0)
		
		#xs = [x[0] for x in curve]
		#ys = [y[1] for y in curve]
		
		#plt.plot(xs, ys)
		#plt.show()
		
		filehandle.write("Curve curve_"+str(i) + "\n")
		for x in curve:
			filehandle.write(str(x[0]) + " " + str(x[1]) + "\n")
		filehandle.write("\n")

		
	






if __name__ == "__main__":
	print 'generating value curves'
	
	
	filename = "big_curves_file.txt"
	
	output_n_curves_to_file(1000, filename)
	
	print "output finished"
	
	exit()
	
	for i in range(15):
		
		curve = random_curve(3, 20, 1.2, 4.0, 10.0)
		xs = [x[0] for x in curve]
		ys = [y[1] for y in curve]

		plt.plot(xs, ys)
		plt.show()
