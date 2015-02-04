#gridsimtesting.py

import unittest
import numpy
from app_man import *
from resource_man import *
from req_man import *
from import_manager import import_manager
import_manager_local = import_manager()
if import_manager_local.import_trace:
	from SimPy.SimulationTrace import *
else:
	from SimPy.Simulation import *


#application testing

class ApplicationTaskInitTester(unittest.TestCase):
	
	def setUp(self):
		class TaskState :
			stalled , ready , running, completed = range(4)
		self.newtask2 = None
		self.newtask = task("task1", 20, "job1", "reqkind1", [], [self.newtask2], 1, {"debugging_on": True})
		self.newtask2 = task("task2", 20, "job1", "reqkind1", [self.newtask], [], 2, {"debugging_on": True})

	def test_task_microkernel_init(self):		
		self.assertEqual(self.newtask.kernels[0].name, "task1_kernel_0")
		self.assertEqual(self.newtask.kernels[0].exectime, 20)
		self.assertEqual(self.newtask.kernels[0].parent_task, self.newtask)
		
		self.assertEqual(self.newtask2.kernels[0].name, "task2_kernel_0")
		self.assertEqual(self.newtask2.kernels[0].exectime, 20)
		self.assertEqual(self.newtask2.kernels[0].parent_task, self.newtask2)
		
		self.assertEqual(self.newtask2.kernels[1].name, "task2_kernel_1")
		self.assertEqual(self.newtask2.kernels[1].exectime, 20)
		self.assertEqual(self.newtask2.kernels[1].parent_task, self.newtask2)
		
	def test_task_init(self):		
		self.assertEqual(self.newtask.taskid, "task1")
		self.assertEqual(self.newtask.exectime, 20)
		self.assertEqual(self.newtask.parent_job, "job1")
		self.assertEqual(self.newtask.get_requirements_kind(), "reqkind1")
		self.assertEqual(len(self.newtask.kernels), 1)
		self.assertEqual(self.newtask.get_core_count(), 1)
		self.assertEqual(len(self.newtask2.kernels), 2)
		self.assertEqual(self.newtask2.get_core_count(), 2)
		
		self.assertEqual(self.newtask.completed, False)
		self.assertEqual(self.newtask.starttime, None)
		self.assertEqual(self.newtask.finishtime, None)
		self.assertEqual(self.newtask.execnodes, None)		
	
	def test_task_dep_init(self):
		self.assertEqual(self.newtask.dependency_manager.dependencies, [])
		self.assertEqual(self.newtask2.dependency_manager.dependencies, [self.newtask])
		
		self.assertEqual(self.newtask.dependencies_satisfied(), True)
		self.assertEqual(self.newtask2.dependencies_satisfied(), False)
	
	
	def test_task_state_init(self):
		self.assertEqual(self.newtask.state_manager.state, TaskState.ready)
		self.assertEqual(self.newtask2.state_manager.state, TaskState.stalled)


class ApplicationJobInitTester(unittest.TestCase):
	
	def setUp(self):
		class TaskState :
			stalled , ready , running, completed = range(4)
		self.newjob = job("job1", {"debugging_on": True})
		self.newjob.set_submission_time(20)
		self.rm = requirements_manager()
		
	def test_job_init(self):
		self.assertEqual(self.newjob.jobid, "job1")
		self.assertEqual(self.newjob.submit_time, 20)
		self.assertEqual(self.newjob.completion_time, None)
		self.assertEqual(self.newjob.activation_time, None)
		self.assertEqual(self.newjob.submit_node, None)
		self.assertEqual(self.newjob.completed, False)
		self.assertEqual(self.newjob.tasklist, None)
		self.assertEqual(self.newjob.task_requirement_kinds, None)
		self.assertEqual(self.newjob.debugging_on, True)
		self.assertEqual(True, self.newjob.task_finish_event is not None)
		
	
	def test_job_tasks_specified(self):
		
		req1 = self.rm.new_kind([("architecture", "x86"), ("graphicskind", "ati")], 
					[("ram", 5), ("disk", 10), ("netbandwidth", 10)])
		
		t1 = None
		t2 = None
		t3 = None
		
		t1 = task("task1", 10, "job1", req1, [], [t2], 1, {"debugging_on": True})
		t2 = task("task2", 20, "job1", req1, [t1], [t3], 3, {"debugging_on": True})
		t3 = task("task3", 30, "job1", req1, [t2], [], 1, {"debugging_on": True})
		
		tasklist = [t1,t2,t3]
		
		self.newjob.set_tasklist(tasklist)
		
		self.assertEqual(len(self.newjob.tasklist), 3)
		self.assertEqual([t.taskid for t in self.newjob.tasklist], ["task1", "task2", "task3"])
		self.assertEqual(self.newjob.completed, False)
		self.assertEqual(self.newjob.task_requirement_kinds, set([req1]))
		self.assertEqual([t.state_manager.state for t in self.newjob.tasklist],
				 [TaskState.ready, TaskState.stalled, TaskState.stalled])
		
		
		
		
	def test_job_assigned(self):
		pass

#need to write tests for the whole of the new cluster class
#need to write tests for pretty much the whole of the refactored 'job' class
#also need to write tests for the refactored application manager class
#to get application manager class working too, need platform manager class going - but possibly leave those bits for another day.
#need to write the (fairly significantly) new Router class in order to get platform manager going
#also need platform generator - or a way of reading in a platform file. (leave generator for another day)




























#Requirements (Heterogeneity) testing

class RequirementsTester(unittest.TestCase):
	
	def setUp(self):
		self.rm = requirements_manager()
	
	def test_req_inits(self):
		self.assertEqual(self.rm.kinds, {})
		
	def test_newkind(self):
		absolute1 = [("architecture", "x86"), ("graphicskind", "ati")]
		relative1 = [("ram", 10), ("disk", 100), ("netbandwidth", 1000)]
		
		a1kind = self.rm.new_kind(absolute1, relative1)
		a1kindcopy = self.rm.new_kind(absolute1, relative1)
		
		self.assertEqual(a1kind, a1kindcopy)
		self.assertEqual(self.rm.get_kind_values(a1kind), 
				{'absolute': {'architecture': 'x86', 'graphicskind': 'ati'},
				 'quantity': {'disk': 100, 'ram': 10, 'netbandwidth': 1000}})
		
	def test_req_kinds_absolutes(self):
		absolute1 = [("architecture", "x86"), ("graphicskind", "ati")]
		absolute2 = [("architecture", "ppc"), ("graphicskind", "nvidia")]
		absolute3 = [("architecture", "x86")]
		
		absolute1_match_subset = [("architecture", "x86")]
		absolute2_match_equal =  [("architecture", "ppc"), ("graphicskind", "nvidia")]
		absolute3_nomatch_superset = [("architecture", "x86"), ("graphicskind", "ati")]
		absolute1_nomatch_different = [("architecture", "ppc"), ("graphicskind", "ati")]	

		a1kind = self.rm.new_kind(absolute1, [])
		a2kind = self.rm.new_kind(absolute2, [])
		a3kind = self.rm.new_kind(absolute3, [])

		a1mskind = self.rm.new_kind(absolute1_match_subset, [])
		a2mekind = self.rm.new_kind(absolute2_match_equal, [])
		a3nskind = self.rm.new_kind(absolute3_nomatch_superset, [])
		a1ndkind = self.rm.new_kind(absolute1_nomatch_different, [])
		
		for i in range(3):
			self.assertEqual(self.rm.requirements_suitable(a1mskind, a1kind), True)
			self.assertEqual(self.rm.requirements_suitable(a2mekind, a2kind), True)
			self.assertEqual(self.rm.requirements_suitable(a3nskind, a3kind), False)
			self.assertEqual(self.rm.requirements_suitable(a1ndkind, a1kind), False)		
		
	
	def test_req_kinds_relatives(self):

		relative1 = [("ram", 10), ("disk", 100), ("netbandwidth", 1000)]
		relative2 = [("ram", 10), ("disk", 100)]
		
		relative1_match_lessthan = [("ram", 5), ("disk", 10)]
		relative1_match_equal = [("ram", 10), ("disk", 100)]
		relative1_nomatch_morethan = [("ram", 50), ("disk", 10)]
		relative2_nomatch_notpresent = [("ram", 1), ("disk", 1), ("netbandwidth", 1)]
		
		r1kind = self.rm.new_kind([], relative1)
		r2kind = self.rm.new_kind([], relative2)
		
		r1mlkind = self.rm.new_kind([], relative1_match_lessthan)
		r1mekind = self.rm.new_kind([], relative1_match_equal)
		r1nmkind = self.rm.new_kind([], relative1_nomatch_morethan)
		r2nnkind = self.rm.new_kind([], relative2_nomatch_notpresent)
		
		#ensure the caching is working properly - run the tests three times
		for i in range(3):
			self.assertEqual(self.rm.requirements_suitable(r1mlkind, r1kind), True)
			self.assertEqual(self.rm.requirements_suitable(r1mekind, r1kind), True)
			self.assertEqual(self.rm.requirements_suitable(r1nmkind, r1kind), False)
			self.assertEqual(self.rm.requirements_suitable(r2nnkind, r2kind), False)
		



if __name__ == "__main__":
	unittest.main()