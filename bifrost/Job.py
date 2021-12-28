from .Work import dcp_init_worker, dcp_compute_worker

from .js_deploy_job import js_deploy_job
from .js_work_function import js_work_function

import cloudpickle
import codecs
import random

import inspect
import re

class Job:

    def __init__(self, input_set, work_function, work_arguments = {}):

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

        # TODO: Move the input checks to top of __init__, and check input attributes directly instead of checking self

        # remote data input checks
        for element in ['input_set', 'work_function', 'work_arguments']:
            if hasattr(self[element], 'remote_data_set'):
                self.remote[element] = 'remote_data_set'
            if hasattr(self[element], 'url_object'):
                self.remote[element] = 'url_object'

        # range object input checks
        if hasattr(self.input_set, 'slices'):
            self.input_set = self.input_set.slices

        # event listener properties
        self.events = {
            'accepted': False,
            'complete': False,
            'console': False,
            'error': False,
            'readystatechange': False,
            'result': True,
        }

        # bifrost internal job properties
        self.python_imports = []
        self.node_js = False
        self.shuffle = False
        self.range_object_input = False

        # work wrapper functions
        self.python_init = dcp_init_worker
        self.python_compute = dcp_compute_worker
        self.python_wrapper = js_work_function

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, item):
        return setattr(self, item)

    def __input_encoder(self, input_data):

        data_encoded = codecs.encode( input_data, 'base64' ).decode()

        return data_encoded

    def __function_writer(self, function):

        try:
            # function code is locally retrievable source code
            function_name = function.__name__    
            function_code = inspect.getsource(function)
        except: # OSError
            try:
                # function code is in a .py file in the current directory
                function_name = function
                function_code = Path(function_name).read_text()
            except: # FileNotFoundError
                # function code is already represented as a string
                function_name = re.findall("def (.+?)\s?\(", function)[0]
                function_code = function
        finally:
            return [function_name, function_code]

    def __module_writer(self, module_name): # TODO: Reconcile with Path().read_text() functionality elsewhere

        module_filename = module_name + '.py'

        with open(module_filename, 'rb') as module:
            module_data = module.read()

        module_encoded = self.__input_encoder( module_data )

        return module_encoded

    def __pickle_jar(self, input_data):

        data_pickled = cloudpickle.dumps( input_data )
        data_encoded = self.__input_encoder( data_pickled )

        return data_encoded

    def __dcp_run(self):

        from bifrost import node

        if self.node_js == True:
            work_arguments_encoded = self.work_arguments # self.__input_encoder(self.work_arguments)
            work_function_encoded = self.work_function # TODO: adapt __function_writer for Node.js files
            work_imports_encoded = {}
        else:
            work_arguments_encoded = self.__pickle_jar(self.work_arguments)
            work_function_encoded = self.__function_writer(self.work_function)
            work_imports_encoded = {}
            for module_name in self.python_imports:
                work_imports_encoded[module_name] = self.__module_writer(module_name)

        input_set_encoded = []
        if self.remote['input_set']:
            if self.remote['input_set'] == 'remote_data_set':
                input_set_encoded = self.input_set.remote_data_set
            elif self.remote['input_set'] == 'url_object':
                input_set_encoded = self.input_set.url_object
            else:
                input_set_encoded = self.input_set
        else:
            for slice_index, input_slice in enumerate(self.input_set):
                slice_object = {
                    'index': slice_index,
                    'data': False,
                }
                if (self.range_object_input == False):
                    if (self.node_js == False):
                        input_slice_encoded = self.__pickle_jar(input_slice)
                    else:
                        input_slice_encoded = input_slice # self.__input_encoder(input_slice)
                    slice_object['data'] = input_slice_encoded
                input_set_encoded.append(slice_object)

        job_input = []
        for i in range(self.multiplier):
            job_input.extend(input_set_encoded)

        if self.shuffle == True:
            random.shuffle(job_input)

        #python_init_source = inspect.getsource(self.python_init)
        #python_compute_source = inspect.getsource(self.python_compute)

        run_parameters = {
            'deploy_function': self.python_wrapper,
            'dcp_data': job_input,
            'dcp_parameters': work_arguments_encoded,
            'dcp_function': work_function_encoded,
            'dcp_multiplier': self.multiplier,
            'dcp_local': self.local_cores,
            'dcp_groups': self.compute_groups,
            'dcp_public': self.public,
            'dcp_requirements': self.requirements,
            'dcp_debug': self.debug,
            'dcp_node_js': self.node_js,
            'dcp_events': self.events,
            'dcp_remote_flags': self.remote,
            'dcp_remote_storage_location': self.remote_storage_location,
            'dcp_remote_storage_params': self.remote_storage_params,
            'python_packages': self.requires,
            'python_modules': work_imports_encoded,
            'python_imports': self.python_imports,
            'python_init_worker': self.python_init,
            'python_compute_worker': self.python_compute,
        }

        node_output = node.run(js_deploy_job, run_parameters)

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
        results = self.__dcp_run()
        self.results = results
        return results

    def local_exec(self, local_cores = 1):
        self.local_cores = local_cores
        results = self.__dcp_run()
        self.results = results
        return results

    def set_slice_payment_offer(self, slice_payment_offer):
        self.slice_payment_offer = slice_payment_offer

    def set_payment_account_keystore(self, payment_account_keystore):
        self.payment_account_keystore = payment_account_keystore

