#useful helper functions

import collections
import datetime

def listalltrue(ls):
	#print list
	if len(ls) == 0:
		return True
	else:
		return reduce(lambda x, y: x and y, ls)
		
		
def listcontainstrue(ls):
	if len(ls) == 0:
		return False
	elif len(ls) == 1:
		return ls[0]
	else:
		return reduce(lambda x, y: x or y, ls)
	

def strip_duplicates_in_list_of_dicts(dictlist):
	newlist = []
	for d in dictlist:
		append = True
		for n in newlist:
			if d == n:
				append = False
		if append:
			newlist.append(d)
	return newlist
	

class idcounter:
	def __init__(self):
		self.count = 0
	
	def newid(self):
		c = self.count
		self.count += 1
		return c


def first_of(value):
	return value[0]


class progress_manager(object):
	def __init__(self, progress_text_prefix, progress_resolution):
		self.name = "progress_manager"
		self.progress_resolution = progress_resolution
		self.progress_text_prefix = progress_text_prefix
		self.tick_count = 0
		self.last_tick_time = datetime.datetime.now()
		self.next_tick = self.progress_resolution
	
	def tick(self):
		self.tick_count += 1
		if self.tick_count == self.next_tick:
			tickdiff = datetime.datetime.now() - self.last_tick_time
			ticksex = tickdiff.seconds
			tickmil = tickdiff.microseconds/1000
			print "\t\t", ticksex, ".", tickmil, "  ",  self.progress_text_prefix, self.tick_count
			self.next_tick += self.progress_resolution
			self.last_tick_time = datetime.datetime.now()



KEY, PREV, NEXT = range(3)

class OrderedSet(collections.MutableSet):

    def __init__(self, iterable=None):
        self.end = end = [] 
        end += [None, end, end]         # sentinel node for doubly linked list
        self.map = {}                   # key --> [key, prev, next]
        if iterable is not None:
            self |= iterable

    def __len__(self):
        return len(self.map)

    def __contains__(self, key):
        return key in self.map

    def add(self, key):
        if key not in self.map:
            end = self.end
            curr = end[PREV]
            curr[NEXT] = end[PREV] = self.map[key] = [key, curr, end]

    def discard(self, key):
        if key in self.map:        
            key, prev, next = self.map.pop(key)
            prev[NEXT] = next
            next[PREV] = prev

    def __iter__(self):
        end = self.end
        curr = end[NEXT]
        while curr is not end:
            yield curr[KEY]
            curr = curr[NEXT]

    def __reversed__(self):
        end = self.end
        curr = end[PREV]
        while curr is not end:
            yield curr[KEY]
            curr = curr[PREV]

    def pop(self, last=True):
        if not self:
            raise KeyError('set is empty')
        key = next(reversed(self)) if last else next(iter(self))
        self.discard(key)
        return key

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, OrderedSet):
            return len(self) == len(other) and list(self) == list(other)
        return set(self) == set(other)

    def __del__(self):
        self.clear()                    # remove circular references
