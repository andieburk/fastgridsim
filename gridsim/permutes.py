'''
A procedure for generating parameter sweep dictionaries.
Supports 3 kinds of inputs:
- invariants
	these are parameters that are replicated unchanged in the output dictionary
	(input format as a dictionary with a single item for each value)
- permuatations
	these are lists of values for parameters for which all permutations should be found
	(input format as a dictionary with a list of items for each value)
- multi-values
	this is a set of paired/tripled/n-led values that are to output together
	(input format as a list of dictionaries. each of these dictionaries all have a list of  the same N parameters for each value. N is the same for each dict, but can differ between dicts)

Andrew Burkimsher
September 2012

'''

import itertools
import unittest


def _check_multi_values_valid(multi_values):
	
	valid = True
	
	if len(multi_values) > 0:
		for mv in multi_values:
			if len(mv.items()) != 0:
				
				multi_lens = [len(m) for m in mv.values()]
				
				for m1 in multi_lens:
					if m1 != multi_lens[0]:
						print "error, mismatched input lengths in multi-values"
						print mv
						valid = False
	return valid








def generate_sweep_from_sweep_ranges(invariants, permutations, multi_values, debugging_on = False):
	
	if not _check_multi_values_valid(multi_values):
		print 'multi values not valid'
		exit()

	#transform the permutation parts into a list of tuples with possible values
	dict_entry_to_list = lambda t : [(t[0],i) for i in t[1]]
	bits_to_permute = [dict_entry_to_list(p) for p in permutations.items()]

	
	#print multi_values

	#preprocess the multiple paired values list where the same parameters are used
	#still not sure what to do with subsets - so currently return an error
	filtered_bits_to_multi = {}

	for mv in multi_values:
		
		mv_keys_set = frozenset(mv.keys())
		
		added = False
		if len(filtered_bits_to_multi) > 0:
			
			for k in filtered_bits_to_multi.values():
				k_keys_set = frozenset(k.keys())
			
				if mv_keys_set < k_keys_set or mv_keys_set > k_keys_set:
					print "error, mismatched subsets in multi-value sweeps"
					print mv_keys_set, k_keys_set
					print mv
					print filtered_bits_to_multi
					exit()
			
				elif (mv_keys_set == k_keys_set):# and not added:
					for matched_key in filtered_bits_to_multi[mv_keys_set].keys():
						filtered_bits_to_multi[mv_keys_set][matched_key].extend(mv[matched_key])
						added = True

		if not added:
			filtered_bits_to_multi[mv_keys_set] = mv

	multi_values = filtered_bits_to_multi.values()

	#transform the multiple paired values list into a list of tuples with possible values
	bits_to_multi = []
	for mv in multi_values:
		this_grouped_set = []
		for i in range(len(mv.values()[0])):
			keys = mv.keys()
			values = [val[i] for val in mv.values()]
			group = zip(keys, values)
			this_grouped_set.append(group)
		bits_to_multi.append(this_grouped_set)
	
	#create the product of the multiple nned values
	final_product_parts_from_multi = []
	for multi_bit in itertools.product(*bits_to_multi):
		#print multi_bit
		line_list = []
		for items in multi_bit:
			line_list.extend(items)
		final_product_parts_from_multi.append(line_list) #work out here - when there are multiple key collisions

	#debugging prints if necessary
	if debugging_on:
		print 'invariant bits', invariants.items()
		print 'bits to permute', bits_to_permute
		print 'bits to multi', bits_to_multi


	deduplicator_set = set([])

	#create the final generator (using a yield gives an iterator to not blow up the RAM with large permutes)
	for final_bit in itertools.product(final_product_parts_from_multi, *bits_to_permute):
		all_parts = final_bit[0]
		all_parts.extend(final_bit[1:])
				
		variants = dict(all_parts)
		output_dict = dict(invariants.items() + variants.items())
		
		dedup_tuple = tuple(output_dict.items())
		
		if dedup_tuple not in deduplicator_set:
			deduplicator_set.add(dedup_tuple)
			yield output_dict
		



class TestSweepGenerator(unittest.TestCase):

	def setUp(self):
		self.invariants = {'a':1, 'b':2, 'c':3}
		self.permutations = {'d': [4,5,6], 'e':[7]}
		self.multi_values = [{'f':[8,9], 'g':[10,11]}, {'h': [15], 'i': ['*']}, {'h':[12,13,14], 'i':['+','-','/']}]

		self.expected_dictionary_values = [
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 4, 'g': 10, 'f': 8, 'i': '+', 'h': 12},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 5, 'g': 10, 'f': 8, 'i': '+', 'h': 12},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 6, 'g': 10, 'f': 8, 'i': '+', 'h': 12},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 4, 'g': 10, 'f': 8, 'i': '-', 'h': 13},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 5, 'g': 10, 'f': 8, 'i': '-', 'h': 13},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 6, 'g': 10, 'f': 8, 'i': '-', 'h': 13},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 4, 'g': 10, 'f': 8, 'i': '/', 'h': 14},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 5, 'g': 10, 'f': 8, 'i': '/', 'h': 14},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 6, 'g': 10, 'f': 8, 'i': '/', 'h': 14},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 4, 'g': 10, 'f': 8, 'i': '*', 'h': 15},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 5, 'g': 10, 'f': 8, 'i': '*', 'h': 15},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 6, 'g': 10, 'f': 8, 'i': '*', 'h': 15},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 4, 'g': 11, 'f': 9, 'i': '+', 'h': 12},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 5, 'g': 11, 'f': 9, 'i': '+', 'h': 12},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 6, 'g': 11, 'f': 9, 'i': '+', 'h': 12},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 4, 'g': 11, 'f': 9, 'i': '-', 'h': 13},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 5, 'g': 11, 'f': 9, 'i': '-', 'h': 13},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 6, 'g': 11, 'f': 9, 'i': '-', 'h': 13},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 4, 'g': 11, 'f': 9, 'i': '/', 'h': 14},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 5, 'g': 11, 'f': 9, 'i': '/', 'h': 14},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 6, 'g': 11, 'f': 9, 'i': '/', 'h': 14},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 4, 'g': 11, 'f': 9, 'i': '*', 'h': 15},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 5, 'g': 11, 'f': 9, 'i': '*', 'h': 15},
			{'a': 1, 'c': 3, 'b': 2, 'e': 7, 'd': 6, 'g': 11, 'f': 9, 'i': '*', 'h': 15}
			]

	def test_all_ok(self):
		output_dict_list = [out_dict for out_dict in
							generate_sweep_from_sweep_ranges(self.invariants,
															 self.permutations,
															 self.multi_values)]
		#for j in output_dict_list:
		#	print j
		
		self.assertEqual(sorted(output_dict_list), sorted(self.expected_dictionary_values))





if __name__ == '__main__':
	unittest.main()


























