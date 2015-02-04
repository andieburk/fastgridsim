#pool runner test

from pool_runner import pool_runner

p = pool_runner()

b = ["echo 'Hello Andrew'", "echo 'Hello Emily'"]

p.parallel_exec_strings(b)