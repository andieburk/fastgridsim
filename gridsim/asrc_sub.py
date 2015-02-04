import sys
import os
import time


def command_basic():


	return "qsub -cwd -V -pe openmpi 1 -l exclusive=false,vf=9G -S /bin/bash -t 001-001 /gpfs/thirdparty/auk-cfms-r/home/aburkimsher/epd-7.3-1-rh5-x86_64/bin/python /gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/gridsim.py "



def write_qsub_file(outpath, id):
	
	idstr = str(id) + '-' + str(id)
	
	f = open(outpath, 'w')
	f.write("#$ -cwd\n")
	f.write("#$ -V\n")
	f.write("#$ -pe openmpi 1\n")
	f.write("#$ -l exclusive=false,vf=9G\n")
	f.write("#$ -S /bin/bash\n")
	f.write("#$ -t " + idstr + "\n\n\n")
	f.write("FILEX=`/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/epd-7.3-1-rh5-x86_64/bin/python /gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/pytestout.py /gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/utilisation_sweep/ $SGE_TASK_ID`\n")
	f.write("echo running $FILEX\n")
	f.write("/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/epd-7.3-1-rh5-x86_64/bin/python /gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/gridsim.py $FILEX\n")


	f.close()





#os.system("module load sge62u5")


in_folder_path = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/utilisation_sweep" #sys.argv[1]
out_folder_path = "/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/utilisation_sweep_jobcards"

			
files = os.listdir(out_folder_path)

sfiles = sorted(files)

count = 0

for id in range(len(sfiles)):
	command = "qsub " + os.path.join(out_folder_path, sfiles[id])
	print "submitting", command
	print os.system(command)
	count += 1
	#if count < 120:
	#	time.sleep(45)

print count, "jobs submitted"
	
"""	
	
	jc_filename = "jobcard_" + str(id+1) + ".jcf"
	file_path = os.path.join(out_folder_path, jc_filename)
	write_qsub_file(file_path, id+1)

"""



"""
	
	
	#file = files[int(sys.argv[2])]
	#print os.path.join(sys.argv[1], file)
	
	
	
	
#$ -cwd
#$ -V
#$ -pe openmpi 1
#$ -l exclusive=false,vf=9G
#$ -S /bin/bash
#$ -t 00003-00003

FILEX=`/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/epd-7.3-1-rh5-x86_64/bin/python /gpfs/thirdparty/auk-cfms-r/home/aburkimsher/asrc/pytestout.py /gpfs/thirdparty$
echo running $FILEX
/gpfs/thirdparty/auk-cfms-r/home/aburkimsher/epd-7.3-1-rh5-x86_64/bin/python /gpfs/thirdparty/auk-cfms-r/home/aburkimsher/GridSim/gridsim/gridsim.py $FILEX
"""



