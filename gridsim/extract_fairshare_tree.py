'''
Created on 15 Feb 2012

@author: aburkimsher
'''


#from FairshareTreeReader import FairshareTreeReader
#from InputProcessing import InputProcessing
#from FairshareTreeMetrics import FairshareTreeMetrics
#from superdome_view_live_analysis import superdome_view_live_analysis
#import math
#import sys
from live_hpc_fairshare_viewer import FairShareViewer




if __name__ == '__main__':

    f = FairShareViewer()
    
    f.read_arguments()
    
    #if f.arguments["filename"] == "":
    f.arguments["filename"] = "./test/all_loio_corrected.txt"

    output_filename = "fairshare_tree_extract_hl.txt"
    
    f.run_the_viewer()
    
    o = open(output_filename, 'w')
    
    for clust_dict in f.model.node_list[0:1]:
        
        print len(clust_dict.items())
        print "\n"
        
        for node in clust_dict.items():
            s = node[1].path + ' ' + str(node[1].shares) + '\n'
            print s
            o.write(s)
        print "\n\n\n"
    
    #print f.model.users
    printed_paths = set([])
    
    unfair_users = []
    
    for username in f.model.users.items():
        #print username
        for usernode in username[1]:
            unclustered_path = '/'.join(usernode.path.split('/')[1:])
            
            if (unclustered_path + usernode.name) not in printed_paths:
                s = unclustered_path + usernode.name + ' ' + str(usernode.shares) + '\n'
                
                print s
                o.write(s)
                printed_paths.add(unclustered_path + usernode.name)
                
                if usernode.shares != 1:
                    unfair_users.append((usernode.path, usernode.name, usernode.shares))
    
    print "\n", unfair_users
    
    o.close()
    #print f.model.users
    
    
