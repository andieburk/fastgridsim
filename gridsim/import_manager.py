#to keep track of all the imports necessary over all the files to do with gridsim

class import_manager(object):
	def __init__(self):
		self.import_trace = False
		self.use_pypy = False