# MODULES

# python standard library
import itertools

# local modules
from .Job import Job

# PROGRAM

class Dcp:

    def __init__(self, scheduler_url = 'https://scheduler.distributed.computer'):
        self.scheduler = scheduler_url
        self.market_rate = 0.0001465376
        self.interactive_js = False

    def compute_do(self, n, work_function, work_arguments = {}): # n is a mandatory argument, in conflict with spec at docs.dcp.dev
        job = Job(list(range(n)), work_function, work_arguments = work_arguments)
        job.range_object_input = True
        job.scheduler = self.scheduler
        job.dcp_install()
        return job

    def compute_for(self, input_set, work_function, work_arguments = {}):
        job = Job(input_set, work_function, work_arguments = work_arguments)
        job.scheduler = self.scheduler
        job.dcp_install()
        return job

    class Url: # Temporary Implementation

        def __init__(self, url_string):
            # from urllib.parse import urlparse
            url_object = url_string # urlparse(url_string)
            self.url_object = url_object

    class RangeObject: # Temporary Implementation

        # TODO: Implement 'group' support
        # TODO: Implement 'sparse' support
        def __init__(self, start, end, step, group = None):
            slices = []
            for value in range(start, end, step):
                slices.append(value)
            self.slices = slices

        def __getitem__(self, item):
            return getattr(self, item)

        def __setitem__(self, item):
            return setattr(self, item)

    class MultiRangeObject: # Temporary Implementation

        # TODO: Avoid transmitting redundant inputs
        def __init__(self, ranges):
            values = []
            for value in ranges:
                if isinstance(value,(list,tuple,set)):
                    values.append(value)
                else:
                    values.append([value])
            slices = []
            for value in itertools.product(*values):
                slices.append(list(value))
            self.slices = slices

        def __getitem__(self, item):
            return getattr(self, item)

        def __setitem__(self, item):
            return setattr(self, item)

    class RemoteDataSet: # Temporary Implementation

        def __init__(self, url_list):
            remote_data_set = []
            for url_string in url_list:
                url_object = url_string # Url(url_string)
                remote_data_set.append(url_object)
            self.remote_data_set = remote_data_set

    class RemoteDataPattern: # Temporary Implementation

        def __init__(self, url_string, url_count):
            remote_data_set = []
            for n in range(url_count):
                url_object = url_string + str(n) # Url(url_string + str(n))
                remote_data_set.append(url_object)
            self.remote_data_set = remote_data_set

