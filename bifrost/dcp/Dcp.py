# MODULES

# python standard library
import itertools

# local modules
from .Job import Job

# PROGRAM

def compute_do(n, work_function, work_arguments = {}): # n is a mandatory argument, in conflict with spec at docs.dcp.dev
    job = Job(list(range(n)), work_function, work_arguments = work_arguments)
    job.range_object_input = True
    return job

def compute_for(input_set, work_function, work_arguments = {}):
    job = Job(input_set, work_function, work_arguments = work_arguments)
    return job

