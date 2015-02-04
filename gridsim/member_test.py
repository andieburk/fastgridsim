#membership testing python

a = [1,2,3,4,5]
b = [3,4,5,6,7]

print 1 in a
print 1 in b

class bob(object):
	def __init__(self,name):
		self.name = name


a = bob("a")
b = bob("b")
d = bob("d")

c = set([a,b])

print a in c
print b in c
print d in c


a = range(1,1000000)
b = range(1,1000000, 2)

results = [blah for blah in b if blah in a]

print results[0:40]