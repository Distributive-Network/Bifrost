import os
from bifrost import dcp
import numpy as np

def work_function(b,N):
  import numpy as np
  x = np.linspace(0,b,N+1)
  x_left_endpoints = x[:-1]
  Delta_x = b/N
  I = Delta_x * np.sum(np.exp(-x_left_endpoints**2))
  return I

input_set = range(25)

shared_arguments = [100000]

job = dcp.compute_for(input_set, work_function, shared_arguments)
job.requires('numpy')
job.public['name'] = "Bifrost DCP Testing : Numpy Riemann Sums"
job.pickle_work_function = False
job.compute_groups = [{'joinKey': 'github-actions', 'joinSecret': os.environ['DCP_CG_PASS']}]

output_set = job.exec()

compare_set = []
for compare_slice in input_set:
  compare_result = work_function(compare_slice, *shared_arguments)
  compare_set.append(compare_result)

output_set = np.array(output_set)
compare_set = np.array(compare_set)

assert np.allclose( output_set, compare_set ), "Arrays are not equal!"
