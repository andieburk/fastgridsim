"""
#Example Platform File

Kind Kind1 architecture:x86,graphics:nvidia;cores:1,ram:4,disk:250

Router Router1 Clust1
Cluster Clust1
Resource Res1 Kind1
Resource Res2 Kind1
Resource Res3 Kind1
Resource Res4 Kind1
Resource Res5 Kind1
Resource Res6 Kind1
Resource Res7 Kind1
Resource Res8 Kind1
Resource Res9 Kind1
Resource ResA Kind1
Resource ResB Kind1
Resource ResC Kind1
Resource ResD Kind1
Resource ResE Kind1
Resource ResF Kind1 
Resource ResG Kind1

"""

def generate_homogeneous_list(number_of_resources, kind_name, count_base):
	lines = ["Resource Res"+str(counter+count_base+1) + " " + kind_name for counter in range(number_of_resources)]
	return lines	

def homo_cluster(num_res, clustname, kind, count_base):
	lines = ["Cluster " + clustname]
	lines.extend(generate_homogeneous_list(num_res, kind, count_base))
	return lines

def homo_grid(num_res_per_clust, num_clusts, kind):
	
	lines = []
	
	clust_names = ["Clust" + str(counter+1) for counter in range(num_clusts)]
	rescount = 0
	lines.append("Router Router1 " + ','.join(clust_names))
	for c in clust_names:
		lines.append("")
		lines.extend(homo_cluster(num_res_per_clust, c, kind, rescount))
		rescount += num_res_per_clust
	return lines

def three_and_one_grid(num_res_per_clust, kind1, kind2):
	lines = []
	
	clust_names = ["Clust" + str(counter+1) for counter in range(4)]
	rescount = 0
	lines.append("Router Router1 " + ','.join(clust_names))
	for c in clust_names[:-1]:
		lines.append("")
		lines.extend(homo_cluster(num_res_per_clust, c, kind1, rescount))
		rescount += num_res_per_clust
	
	lines.append("")
	lines.extend(homo_cluster(num_res_per_clust, clust_names[-1], kind2, rescount))
	rescount += num_res_per_clust
	return lines

def three_and_one_grid_easier(num_res_per_clust, kind1, kind2):
	lines = []
	
	clust_names = ["Clust" + str(counter+1) for counter in range(2)]
	rescount = 0
	lines.append("Router Router1 " + ','.join(clust_names))
	for c in clust_names[:-1]:
		lines.append("")
		lines.extend(homo_cluster(3000, c, kind1, rescount))
		rescount += 3000
	
	lines.append("")
	lines.extend(homo_cluster(1000, clust_names[-1], kind2, rescount))
	rescount += num_res_per_clust
	return lines

def real_grid(num_res_per_clust, kind1, kind2):
	lines = []
	
	clust_names = ["Clust" + str(counter+1) for counter in range(3)]
	rescount = 0
	lines.append("Router Router1 " + ','.join(clust_names))
	for c in clust_names[:-1]:
		lines.append("")
		lines.extend(homo_cluster(2000, c, kind1, rescount))
		rescount += 2000
	
	lines.append("")
	lines.extend(homo_cluster(1000, clust_names[-1], kind2, rescount))
	rescount += num_res_per_clust
	return lines







#for line in homo_grid(10, 3, "Kind1"):
#	print line

#big_clust = homo_grid(1000, 4, "Kind1")

big_clust = real_grid(1000, "Kind1", "Kind2")

big_clust_lines = [l+"\n" for l in big_clust]


outf = open("platform_real2.txt", 'w')



outf.write("Kind Kind1 architecture:x86,graphics:nvidia;cores:1,ram:4,disk:250\n\n")
outf.write("Kind Kind2 architecture:sparc,graphics:nvidia;cores:1,ram:4,disk:250\n\n")
outf.writelines(big_clust_lines)
outf.close()
	