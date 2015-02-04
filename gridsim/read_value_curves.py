#read in curve definition file
from schedulers import value_curve_valid




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
						curves[curr_curve_name] = curve_point_list
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
		curves[curr_curve_name] = curve_point_list


	return curves



if __name__ == "__main__":
	curves = read_curves_file("/Users/andy/Documents/Work/gridsimxcode/GridSim/gridsim/value_curves.txt")

	print len(curves), "curves read"
	for c in curves:
		print curves




