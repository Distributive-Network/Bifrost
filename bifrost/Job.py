from .Work import Work

import cloudpickle
import codecs
import random

class Job:

    def __init__(self, input_set, work_function, work_arguments = [], scheduler = 'https://scheduler.distributed.computer'):

        # mandatory job arguments
        self.input_set = input_set
        self.work_function = work_function
        self.work_arguments = work_arguments

        # standard job properties
        self.requirements = { 'discrete': False }
        self.initial_slice_profile = {} # Not Used
        self.slice_payment_offer = False # TODO
        self.payment_account = False # TODO
        self.requires = []
        self.require_path = [] # Not Used
        self.module_path = [] # Not Used
        self.collate_results = True
        self.status = { # Not Used
            'run_status': False,
            'total': 0,
            'distributed': 0,
            'computed': 0,
        }
        self.public = {
            'name': 'Bifrost Deployment',
            'description': False,
            'link': False,
        }
        self.context_id = False # Not Used
        self.scheduler = scheduler # TODO
        self.bank = False # Not Used

        # additional job properties
        self.compute_groups = []
        self.debug = False
        self.multiplier = 1
        self.local_cores = 0

        # remote data properties
        self.remote_storage_location = False # TODO
        self.remote_storage_params = False # TODO
        self.remote = { # Bifrost Alternative
            'input_set': False, # list || RemoteDataSet || RemoteDataPattern
            'work_function': False, # string || Url
            'work_arguments': False, # list
            'results': False, # string || Url
        }

        # event listener properties
        self.events = {
            'accept': False,
            'complete': False,
            'console': False,
            'error': False,
            'readystatechange': False,
            'result': False,
        }

        # bifrost internal job properties
        self.python_imports = []
        self.nodejs = False
        self.shuffle = False
        self.range_object_input = False

    def __input_encoder(input_data):

        data_encoded = codecs.encode( input_data, 'base64' ).decode()

        return data_encoded

    def __function_writer(function):

        import inspect

        function_name = function.__name__
        function_code = inspect.getsource(function)

        return [function_name, function_code]

    def __module_writer(module_name):

        module_filename = module_name + '.py'

        with open(module_filename, 'rb') as module:
            module_data = module.read()

        module_encoded = __input_encoder( module_data )

        return module_encoded

    def __pickle_jar(input_data):

        data_pickled = cloudpickle.dumps( input_data )
        data_encoded = __input_encoder( data_pickled )

        return data_encoded

    def __dcp_run(self): # Under Construction

        job_slices = _dcp_slices
        job_function = _dcp_function
        job_arguments = _dcp_arguments
        job_packages = _dcp_packages
        job_groups = _dcp_groups
        job_imports = _dcp_imports
        job_public = _dcp_public
        job_local = _dcp_local
        job_multiplier = _dcp_multiplier
        job_nodejs = _dcp_nodejs
                
        job_input = []
        for i in range(job_multiplier):
            job_input.extend(job_slices_encoded)

        if self.shuffle == True:
            random.shuffle(job_input)

        return job_input

    def dcp_run(
        job_input,
        job_arguments,
        job_function,
        job_packages,
        job_groups,
        job_imports,
        job_modules,
        job_public,
        job_multiplier,
        job_local,
        job_nodejs,
    ):

        work = Work()

        dcp_init_worker = work.dcp_init_worker
        dcp_compute_worker = work.dcp_compute_worker

        run_parameters = {
            'dcp_data': job_input,
            'dcp_multiplier': job_multiplier,
            'dcp_local': job_local,
            'dcp_groups': job_groups,
            'dcp_public': job_public,
            'python_init_worker': dcp_init_worker,
            'python_compute_worker': dcp_compute_worker,
            'python_parameters': job_arguments,
            'python_function': job_function,
            'python_packages': job_packages,
            'python_modules': job_modules,
            'python_imports': job_imports,
        }

        node_output = node.run('./dcp.js', run_parameters)

        job_output = _node_output['jobOutput']
        
        return job_output

    def __js_deploy(self): # Under Construction

        """
        self.input_set
        self.work_function
        self.work_arguments
        self.requires
        self.compute_groups
        self.python_imports
        self.public
        self.local_cores
        self.multiplier
        """

        job_arguments = __input_encoder(_job_arguments)

        job_slices_encoded = []
        for block_index, block_slice in enumerate(job_slices):

            block_slice_encoded = __input_encoder(block_slice)

            job_slices_encoded.append({
                'index': block_index,
                'data': block_slice_encoded })

        job_results = __dcp_run(
            job_input,
            job_arguments,
            job_function,
            job_packages,
            job_groups,
            job_public,
            job_multiplier,
            job_local,
        )

    def __py_deploy(self): # Under Construction

        """
        self.input_set
        self.work_function
        self.work_arguments
        self.requires
        self.compute_groups
        self.python_imports
        self.public
        self.local_cores
        self.multiplier
        """

        job_modules = {}
        for module_name in job_imports:
            job_modules[module_name] = __module_writer(module_name)

        job_function = __function_writer(job_function)

        job_arguments = __pickle_jar(job_arguments)

        job_slices_encoded = []
        for block_index, block_slice in enumerate(job_slices):

            block_slice_encoded = __pickle_jar(block_slice)

            job_slices_encoded.append({
                'index': block_index,
                'data': block_slice_encoded })

        job_results = __dcp_run(
            job_input,
            job_arguments,
            job_function,
            job_packages,
            job_groups,
            job_imports,
            job_modules,
            job_public,
            job_multiplier,
            job_local,
        )

    def on(self, event_name, event_function):
        self.events[event_name] = event_function

    def add_listener(self, event_name, event_function):
        on(self, event_name, event_function)

    def add_event_listener(self, event_name, event_function):
        on(self, event_name, event_function)

    def requires(self, package_name):
        self.requires.append(package_name)

    def set_result_storage(self, remote_storage_location, remote_storage_params = {}):
        self.remote_storage_location = remote_storage_location
        self.remote_storage_params = remote_storage_params
        
    def on(self, event_name, event_function):
        self.events[event_name] = event_function

    def exec(self, slice_payment_offer = self.slice_payment_offer, payment_account = self.payment_account, initial_slice_profile = self.initial_slice_profile):
        self.results = job_deploy(self)

    def local_exec(self, local_cores = 1):
        self.local_cores = local_cores
        self.results = job_deploy(self)

    def set_slice_payment_offer(self, slice_payment_offer):
        self.slice_payment_offer = slice_payment_offer

    def set_payment_account_keystore(self, payment_account_keystore):
        self.payment_account_keystore = payment_account_keystore

