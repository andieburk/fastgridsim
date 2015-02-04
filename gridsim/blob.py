def dep\_ind\_3(nodecount, edgecount):
	#check edge count makes sense
	maxedges = nodecount * (nodecount - 1)
	
	if edgecount > maxedges:
		print "too many edges requested"
	if edgecount == maxedges:
		#complete graph - calc directly
		deplist = [range(i) for i in range(nodecount)][::-1]
		return deplist
	
	#higher loop to catch really impossible distribs
	iter1 = 0
	fin1 = False
	
	
	while (not fin1) and (iter1 < 10):
		iter1 += 1
		fin1 = True
		
		ins\_by\_node = sorted(UUnifast\_int(nodecount-1, edgecount) + [0])
		outs\_by\_node = sorted(UUnifastInteger(nodecount-1, edgecount)  + [0])[::-1]
		
		#iter control
		finished = False
		itercount = 0
		
		#inner loop to try many different allocations. random isn't always perfect
		while (not finished) and itercount <= 40:
			finished = True
			outcounts = \{x: outs\_by\_node[x] for x in range(nodecount)\}
			incounts = \{x: ins\_by\_node[x] for x in range(nodecount)\}
			
			edges = []
			itercount += 1
			
			while len(edges) < edgecount:
				largest\_node\_needing\_an\_in = max([x[0] for x in incounts.items() if x[1] > 0])
				possible\_outs = [x[0] for x in outcounts.items() if (x[1] > 0) and (x[0] < largest\_node\_needing\_an\_in)]
				
				#all\_new\_edges = set([(p, largest\_node\_needing\_an\_in) for p in possible\_outs])
				#possible\_edges = all\_new\_edges - edges
				
				
				possible\_edges = [(p, largest\_node\_needing\_an\_in) for p in possible\_outs if (p, largest\_node\_needing\_an\_in) not in edges]
				
				#print sorted(list(ps))
				#print possible\_edges
				#exit()
				
				if len(possible\_edges) == 0:
					finished = False
					break
				else:
					new\_edge = random.choice(list(possible\_edges))
					outcounts[new\_edge[0]] -= 1
					incounts[new\_edge[1]] -= 1
					edges.append(new\_edge)
		
		if len(edges) < edgecount:
			fin1 = False
	
	if len(edges) < edgecount:
		print "no edge allocation found even after 40 tries with 10 distribs.\nare the input values sane?"
		print 'nodes requested', nodecount
		print 'edges requested', edgecount
		exit()
	
	
	deplist = [[] for i in range(nodecount)]
	for e in edges:
		deplist[e[1]].append(e[0])
	"""
		#sanity check topologically sorted
		for i in range(len(deplist)):
		if len(deplist[i]) > 1:
		if max(deplist[i]) >= i:
		print "not topologically sorted"
		for i in range(len(deplist)):
		print i, deplist[i]
		exit()
		"""
	#print iter1 * itercount
	return deplist