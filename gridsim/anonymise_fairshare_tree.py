'''
Created on 15 Feb 2012

@author: aburkimsher
'''

import collections

class unique_id(object):
    def __init__(self, prefix = None):
        self.base = 1
        if prefix != None:
            self.prefix = prefix
        else:
            self.prefix = ""
    
    def newid(self):
        news = self.prefix + str(self.base)
        self.base += 1
        return news


if __name__ == '__main__':
    input_fairshare_file = "fairshare_tree_extract_2.txt"
    anonymity_mapper_filename = "anon_map2.txt"
    anonymised_file = "anon_fairshare_tree2.txt"
    
    anon_id_root = "id_"
    
    id_counter = unique_id(anon_id_root) 
    
    f = open(input_fairshare_file, 'r')
    
    fairshare_conts = f.readlines()
    
    f.close()
    
    of = open(anonymised_file, 'w')
    
    anon_dict = collections.defaultdict(lambda: id_counter.newid())
    
    
    for line in fairshare_conts:
        ls = line.split(' ')
        shr = ls[1]
        pathparts = ls[0].split('/')
        
        if pathparts[1] == "ug_all_loio":
        
            new_path_parts = [anon_dict[p] for p in pathparts]
            new_path = '/'.join(new_path_parts)
            
            s = new_path + ' ' + shr
            print s
            of.write(s)
    
    of.close()
    
    print anon_dict.items()
    
    an = open(anonymity_mapper_filename, 'w')
    
    for a_pair in sorted(anon_dict.items(), key=lambda x: x[1]):
        s = a_pair[1] + ' ' + a_pair[0] + '\n'
        print s
        an.write(s)

    
    an.close()
    
    print 'done'
    
    