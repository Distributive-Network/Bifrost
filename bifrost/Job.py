from .Work import dcp_init_worker, dcp_compute_worker

import cloudpickle
import codecs
import random

class Job:

    def __init__(self, input_set, work_function, work_arguments = []):

        # mandatory job arguments
        self.input_set = input_set
        self.work_function = work_function
        self.work_arguments = work_arguments

        # standard job properties
        self.requirements = { 'discrete': False }
        self.initial_slice_profile = False # Not Used
        self.slice_payment_offer = False # TODO
        self.payment_account = False # TODO
        self.requires = []
        self.require_path = False # Not Used
        self.module_path = False # Not Used
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
        self.scheduler = 'https://scheduler.distributed.computer' # TODO
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

    def __input_encoder(self, input_data):

        data_encoded = codecs.encode( input_data, 'base64' ).decode()

        return data_encoded

    def __function_writer(self, function):

        import inspect

        function_name = function.__name__
        function_code = inspect.getsource(function)

        return [function_name, function_code]

    def __module_writer(self, module_name):

        module_filename = module_name + '.py'

        with open(module_filename, 'rb') as module:
            module_data = module.read()

        module_encoded = __input_encoder( module_data )

        return module_encoded

    def __pickle_jar(self, input_data):

        data_pickled = cloudpickle.dumps( input_data )
        data_encoded = __input_encoder( data_pickled )

        return data_encoded

    def __dcp_run(self):

        work_function = __function_writer(self.work_function)

        work_arguments_encoded = __pickle_jar(self.work_arguments)
        work_arguments_encoded = __input_encoder(self.work_arguments)

        python_modules = {}
        for module_name in self.python_imports:
            python_modules[module_name] = __module_writer(module_name)

        input_set_encoded = []
        for slice_index, input_slice in enumerate(self.input_set):
            slice_object = {
                'index': slice_index,
                'data': False,
            }
            if (self.range_object_input == False):
                if (self.node_js == False):
                    input_slice_encoded = __pickle_jar(input_slice)
                else:
                    input_slice_encoded = __input_encoder(input_slice)
                slice_object['data'] = input_slice_encoded
            input_set_encoded.append(slice_object)

        job_input = []
        for i in range(self.job_multiplier):
            job_input.extend(self.input_set_encoded)

        if self.shuffle == True:
            random.shuffle(job_input)

        run_parameters = {
            'dcp_data': job_input,
            'dcp_multiplier': self.multiplier,
            'dcp_local': self.local_cores,
            'dcp_groups': self.compute_groups,
            'dcp_public': self.public,
            'python_init_worker': dcp_init_worker,
            'python_compute_worker': dcp_compute_worker,
            'python_parameters': work_arguments_encoded,
            'python_function': work_function,
            'python_packages': self.requires,
            'python_modules': python_modules,
            'python_imports': python_imports,
        }

        node_output = node.run('./dcp.js', run_parameters)

        result_set = node_output['jobOutput']

        self.result_set = result_set
        
        return result_set

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

    def exec(self, slice_payment_offer = False, payment_account = False, initial_slice_profile = False):
        if ( slice_payment_offer != False ):
            self.slice_payment_offer = slice_payment_offer
        if ( payment_account != False ):
            self.payment_account = payment_account
        if ( initial_slice_profile != False ):
            self.initial_slice_profile = initial_slice_profile     
        self.results = __dcp_run(self)

    def local_exec(self, local_cores = 1):
        self.local_cores = local_cores
        self.results = __dcp_run(self)

    def set_slice_payment_offer(self, slice_payment_offer):
        self.slice_payment_offer = slice_payment_offer

    def set_payment_account_keystore(self, payment_account_keystore):
        self.payment_account_keystore = payment_account_keystore

