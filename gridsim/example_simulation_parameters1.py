"faster_less_accurate_charting" : False
"fairshare_enabled" : False
"visualise_each_task" : False
"target_utilisation" : 30
"workload_filename" : "/Users/andie/Desktop/w2/workload0.txt"
#"workload_filename" : "workload9_10s.txt"
#"platform_filename" : "platform_simple.txt"
#"platform_filename" : "platform1000cores.plat"
"platform_filename" : "platform_real2.txt"
"print_metrics_by_job" : False
"debugging_on" : False
"schedule_output_filename" : "depprob4.csv"
"faster_charting_factor" : 1000000
#"cluster_orderer" : "projected_value_remaining_down"
#"cluster_orderer" : "pslr_sf"
#"cluster_orderer"  : "fifo_task"
"cluster_orderer" : "projected_slr"
#"cluster_orderer" : "projected_value_straight_up"
#"cluster_orderer" : "projected_value_density_down"
#"cluster_orderer" : "texec_sum_value_density_down"
#"cluster_orderer" : "longest_remaining_first"
#"cluster_orderer" : "shortest_remaining_first"
#"cluster_orderer" : "pvd_sq_up"
"cumulative_completion_base" : 1350000000
"simulation_max_time" : 1000000000000
"use_supplied_submit_times" : False
"fairshare_tree_filename" : "fairshares1.txt"
"max_job_count" : 20
"staggered_release" : True
"print_global_metrics" : True
"cluster_allocator" : "first_free"
"output_schedule_to_file" : False
"visualisation_enabled" : False
"router_orderer" : "fifo_job"
"router_allocator" : "load_balancing_allocator"
"networking_enabled" : True
"network_delay_level_power" : 2
"communication_to_computation_ratio" : 0.01
"proportion_of_kind2" : 0.0
"exec_time_estimate_kind" : "exact"
"exec_time_estimate_value" : 1000000
"pickle_output_path" : "/Users/andy/Desktop/"
#"metrics_out_formats" : ["text_human", "pickle"]
"metrics_out_formats" : ["text_human"]
"value_enabled" : True
"curves_filename" : "/Users/andy/Documents/Work/gridsimxcode/GridSim/gridsim/big_curves_file.txt"
"job_exec_timeout_factor" : 10
"random_seed" : 123456
"print_schedule" : True
"arrival_pattern" : "day_week_pattern"
"timeouts_enabled" : True
"day_distribution_file" : '/Users/andy/Documents/Work/gridsimxcode/GridSim/gridsim/day_distribution_pickled.txt'
"week_distribution_file" : '/Users/andy/Documents/Work/gridsimxcode/GridSim/gridsim/week_distribution_pickled.txt'