#generate_one_workload

python workload_generator.py --numjobs 64 --totalexecsum 10000000 --minchains 2 --maxchains 20 --minchainlength 3 --maxchainlength 10 --minblocks 2 --maxblocks 5 --randomseed 123606 --outputfile ../grid_sim_run_files/Workload/chain_of_independent_chains_29.txt --depprob 1.0 --workloadstructure 2