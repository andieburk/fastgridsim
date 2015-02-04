'''
Created on 15 Feb 2012

@author: aburkimsher
'''

if __name__ == '__main__':
    input_workload_file = "jobs_log_extract_hl.txt"
    anonymity_mapper_filename = "anon_map2.txt"
    anonymised_file = "anon_workload_hl.txt"
    
    
    f = open(input_workload_file, 'r')
    
    lines = f.readlines()
    
    f.close
    
    af = open(anonymity_mapper_filename)
    
    
    anon_maps = af.readlines()
    
    #print anon_maps
    
    af.close()
    
    
    anon_dict = {}
    
    
    for a in anon_maps:
        asplit = a.rstrip().split(' ')
        
        #print asplit
        
        if len(asplit) < 2:
            key = ' '
        else:
            key = asplit[1]
        
        anon_dict[key] = asplit[0]
        
    #print anon_dict
    
    
    
    
    
    lines2 = []
    
    for l in lines:
        
        if l[:3] == "Job":
            lsplit = l.rstrip().split(' ')
            pathsplit = lsplit[3].split("/")
            
            #print pathsplit
            
            
            anon_path_bits = [anon_dict[p] for p in pathsplit]
            
            anon_path = '/'.join(anon_path_bits)
            
            nl = lsplit[:3]
            nl.append(anon_path)
            
            
            #print nl
            
            newline = ' '.join(nl) + '\n'
            
            lines2.append(newline)
            
        
        else:
            lines2.append(l)
        
    
    wf = open(anonymised_file, 'w')
    
    for l in lines2:
        wf.write(l)
    
    wf.close()
    print "done.."
    
    #print lines2
        
        