from SimPy.SimulationTrace import *

class p(Process):
	def __init__(self, name, exectime):
		Process.__init__(self, name=name)
		self.exectime = exectime
	
	def go(self, resource):
		print "hello_world"
		yield request, self, resource
		yield hold, self, self.exectime
		yield release, self, resource
		print "bye world", now()


print "init"
initialize()
res = Resource(capacity=1, name="Res1", unitName="core", preemptable=False, monitored=True)

pp = p("bob", 10)
print "created, now activating"
activate(pp, pp.go(res), at=now())

simulate(until=1000)

print "finished at", now()

exit()
