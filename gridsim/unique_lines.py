import os
import re


folder_base = "/Users/andy/Documents/"
#folder_base = "/Volumes/NO NAME/pastview_analysis_visualisation"


#print os.walk(folder_base)

#walk_bits = [f for f in os.walk(folder_base)]# if f[-3:] == "py"]


all_files = []


for f in os.walk(folder_base):
	#folder_count += 1
	
	#print f[2]
	
	
	newfiles = [os.path.join(f[0], fi) for fi in f[2] if fi[0] != "." and fi[-3:] == ".py"]
	
	all_files.extend(newfiles)
	



#all_files = [f for f in walk_bits[0][2] if f[-2:] == "py"]

print all_files

all_lines_set = set([])


for f in all_files:
	fpath = f
	
	f_handler = open(fpath, 'r')
	
	file_lines_set = set(f_handler.readlines())
	
	all_lines_set = all_lines_set | file_lines_set

print len(all_lines_set)