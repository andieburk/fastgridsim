# req_man.py

from helpers import *

class requirements_manager():
	
	'''every set of requirements will be known as a unique 'kind'
	working with these strings is intended to be much more efficient than whole dicts all over the place
	when operations need to actually be performed between kinds, they should really happen inside a req_manager
	rather than passing loads of dicts around!'''
	
	def __init__(self):
		self.memoising_dict = {}
		self.unnamed_kind_counter = idcounter()
		self.kinds = {}
		self.hitcount = 0
		self.misscount = 0
	
	def new_kind(self, absolute, quantity, kind_name=""):

		possible_new = dict([('absolute', dict(absolute)), ('quantity', dict(quantity))])
		
		"""
		#check if there is an equivalent kind already present.
		all_values = self.kinds.values()
		
		for i in range(len(all_values)):
			#if equivalent exists, then return the one already there
			
			if possible_new == all_values[i]:
				return self.kinds.keys()[i]
		"""
		#otherwise generate a new one and return the new kind id
		if kind_name == "":
			newkindid = "kind_" + str(self.unnamed_kind_counter.newid())
		else:
			newkindid = kind_name
			
		self.kinds[newkindid] = possible_new
		
		return newkindid
	
	
	def get_kind_values(self, kind_id):
		return self.kinds[kind_id]

	def set_of_requirements_satisfied(self, required_kinds, available_kinds):
		
		if required_kinds.issubset(available_kinds):
			return True
		
		sat_all = True
		for r in required_kinds:
			sat_one = False
			for a in available_kinds:
				sat_one = sat_one or self.requirements_suitable(r, a)
			sat_all = sat_all and sat_one
		return sat_all
	
	def requirements_suitable(self, required_kind, available_kind):
		#use the memoised value if it is present to save time!
		if (required_kind, available_kind) in self.memoising_dict:
			self.hitcount += 1
			return self.memoising_dict[(required_kind, available_kind)]
		self.misscount += 1

		#are all the absolute requirements present?
		if not set(self.kinds[required_kind]['absolute'].keys()).issubset(set(self.kinds[available_kind]['absolute'].keys())):
			self.memoising_dict[(required_kind, available_kind)] = False
			return False
		
		#are all the absolute requirements of the right type?
		for k in self.kinds[required_kind]['absolute'].keys():
			if self.kinds[required_kind]['absolute'][k] != self.kinds[available_kind]['absolute'][k]:
				self.memoising_dict[(required_kind, available_kind)] = False
				return False
		
		#are all the quantity requirements present?
		if not set(self.kinds[required_kind]['quantity'].keys()).issubset(set(self.kinds[available_kind]['quantity'].keys())) :
			self.memoising_dict[(required_kind, available_kind)] = False
			return False
				
		#are all the quantity requirements sufficient?
		for k in self.kinds[required_kind]['quantity'].keys():
			if self.kinds[required_kind]['quantity'][k] > self.kinds[available_kind]['quantity'][k]:
				self.memoising_dict[(required_kind, available_kind)] = False
				return False
		
		#if all the other conditions have been satisfied, return True
		self.memoising_dict[(required_kind, available_kind)] = True
		return True
