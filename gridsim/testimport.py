"""sim_params = {}
sim_params["loader"] = "SimPy.SimulationTrace"

if sim_params["loader"] == "SimPy.SimulationTrace":
	from SimPy.SimulationTrace import *
else:
	from SimPy.Simulation import *
"""
from import_manager import import_manager
import_manager_local = import_manager()
if import_manager_local.import_trace:
	from SimPy.SimulationTrace import *
else:
	from SimPy.Simulation import *