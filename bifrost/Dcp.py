from .Job import Job

class Dcp:

    def __init__(self, scheduler_url = 'https://scheduler.distributed.computer'):
        self.scheduler = scheduler_url
        self.market_rate = 0.0001465376
        self.interactive_js = False

    def compute_do(self, n, work_function, work_arguments = []): # n is a mandatory argument, in conflict with spec at docs.dcp.dev
        job = Job(list(range(n)), work_function, work_arguments = work_arguments)
        job.scheduler = self.scheduler
        job.range_object_input = True
        return job

    def compute_for(self, input_set, work_function, work_arguments = []):
        job = Job(input_set, work_function, work_arguments = work_arguments)
        job.scheduler = self.scheduler
        return job

    class Url: # Temporary Implementation

        def __init__(self, url_string):
            # from urllib.parse import urlparse
            url_object = url_string # urlparse(url_string)
            self.url_object = url_object

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

