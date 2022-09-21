# MODULES

# python standard library
import codecs
import inspect
import pickle
import random
import re
import zlib
from pathlib import Path

# pypi modules
import cloudpickle

# local modules
from bifrost.py_utils import is_colab
from .Work import dcp_init_worker, dcp_compute_worker, js_work_function, js_deploy_job

# PROGRAM

class Job:

    def __init__(self, input_set, work_function, work_arguments = [], work_keyword_arguments = {}):

        # mandatory job arguments
        self.input_set = input_set
        self.work_function = work_function

        # alternative job arguments
        self.work_arguments = work_arguments # positional args, iterable, provided to work function in order
        self.work_keyword_arguments = work_keyword_arguments # named args, dict, provided to work function after args

        # standard job properties
        self.requirements = { 'discrete': False }
        self.initial_slice_profile = False # Not Used
        self.slice_payment_offer = False # TODO
        self.payment_account = False # TODO
        self.require_path = [] # dcp pyodide packages (populated via Job.requires)
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
        self.id = None # Assigned on job.accepted

        # additional job properties
        self.collate_results = True
        self.compute_groups = []
        self.debug = False
        self.estimation_slices = 3
        self.greedy_estimation = False
        self.multiplier = 1
        self.local_cores = 0

        # file system api
        self.files_data = {}
        self.files_path = []
        self.input_set_files = False

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
            'accepted': True,
            'complete': True,
            'console': True,
            'error': True,
            'readystatechange': True,
            'result': True,
        }

        # bifrost internal job properties
        self.python_imports = [] # local python modules (populated via Job.imports)
        self.node_js = False
        self.shuffle = False
        self.range_object_input = False
        self.pickle_work_function = True
        self.pickle_work_arguments = True
        self.pickle_input_set = True
        self.pickle_output_set = True
        # TODO: more robust integration of non-pickled encoding
        self.encode_work_function = True
        self.encode_work_arguments = True
        self.encode_input_set = True
        self.encode_output_set = True
        self.compress_work_function = True
        self.compress_work_arguments = True
        self.compress_input_set = True
        self.compress_output_set = True
        self.new_context = False # clears the nodejs stream after every job if true
        self.kvin = False # uses the kvin serialization library to decode job results
        self.cloudpickle = True # use non-cloud pickling for colab deployment
        self.pyodide_wheels = False # use newer version of pyodide which uses .whl packages
        self.show_timings = False # per-slice worker, per-slice client, and total overall

        # work wrapper functions
        self.python_init = dcp_init_worker
        self.python_compute = dcp_compute_worker
        self.python_wrapper = js_work_function
        self.python_deploy = js_deploy_job

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, item):
        return setattr(self, item)

    def __input_encoder(self, input_data):

        data_encoded = codecs.encode( input_data, 'base64' ).decode()

        return data_encoded

    def __output_decoder(self, output_data):

        data_decoded = codecs.decode( output_data.encode(), 'base64' )

        return data_decoded

    def __pickle_jar(self, input_data, compress_data = False):

        import bifrost

        if is_colab() or self.cloudpickle == False:
            data_pickled = pickle.dumps( input_data, protocol=4 )
        else:
            if hasattr(cloudpickle, 'register_pickle_by_value'):
                cloudpickle.register_pickle_by_value(bifrost)
            data_pickled = cloudpickle.dumps( input_data )
        if compress_data == True:
            data_compressed = zlib.compress( data_pickled )
            data_encoded = self.__input_encoder( data_compressed )
        else:
            data_encoded = self.__input_encoder( data_pickled )

        return data_encoded

    def __unpickle_jar(self, output_data, decompress_data = False):

        data_decoded = self.__output_decoder( output_data )
        if decompress_data == True:
            data_decompressed = zlib.decompress( data_decoded )
            if is_colab() or self.cloudpickle == False:
                data_unpickled = pickle.loads( data_decompressed )
            else:
                data_unpickled = cloudpickle.loads( data_decompressed )
        else:
            if is_colab() or self.cloudpickle == False:
                data_unpickled = pickle.loads( data_decoded )
            else:
                data_unpickled = cloudpickle.loads( data_decoded )

        return data_unpickled

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
            return {'name': function_name, 'code': function_code}

    def __module_writer(self, module_name): # TODO: Reconcile with Path().read_text() functionality elsewhere

        module_filename = module_name + '.py'

        with open(module_filename, 'rb') as module:
            module_data = module.read()

        module_encoded = self.__input_encoder( module_data )

        return module_encoded

    def __file_writer(self, file_name):

        with open(file_name, 'rb') as file_handle:
            file_data = file_handle.read()

        file_encoded = self.__input_encoder( file_data )

        return file_encoded

    def __dcp_run(self):

        from bifrost import node

        if self.input_set_files == True:
            self.pickle_work_function = False

        if len(self.files_data) > 0:
            self.pickle_work_function = False

        if self.pyodide_wheels == True:
            self.cloudpickle = False
            self.pickle_output_set = True

        if is_colab():
            self.cloudpickle = False

        if self.cloudpickle == False:
            self.pickle_work_function = False

        if self.node_js == True:
            work_arguments_encoded = False # self.__input_encoder(self.work_arguments)
            work_keyword_arguments_encoded = False

            if len(self.work_keyword_arguments) > 0:
                self.work_arguments.append(self.work_key_arguments)

            node.run("""
            globalThis.nodeSharedArguments = [];
            """)

            for argument_index in range(len(self.work_arguments)):
                shared_argument = self.work_arguments[argument_index]
                node.run("""
                nodeSharedArguments.push( sharedArgument );
                """, { 'sharedArgument': shared_argument })

            # XXX: named arguments are not supported in JS, so the best we can do is treat them as positionals
            for argument_keyword in self.work_keyword_arguments:
                named_argument = self.work_keyword_arguments[argument_keyword]
                node.run("""
                nodeSharedArguments.push( named_argument );
                """, { 'namedArgument': named_argument })

            work_function_encoded = self.work_function # TODO: adapt __function_writer for Node.js files
            work_imports_encoded = {}
        else:
            if self.pickle_work_arguments == True:
                work_arguments_encoded = self.__pickle_jar(self.work_arguments, self.compress_work_arguments)
                work_keyword_arguments_encoded = self.__pickle_jar(self.work_keyword_arguments, self.compress_work_arguments)
            elif self.encode_work_arguments == True:
                work_arguments_encoded = self.__input_encoder(self.work_arguments)
                work_keyword_arguments_encoded = self.__input_encoder(self.work_keyword_arguments)
            else:
                work_arguments_encoded = self.work_arguments
                work_keyword_arguments_encoded = self.work_keyword_arguments
            if self.pickle_work_function == True:
                work_function_encoded = self.__pickle_jar(self.work_function, self.compress_work_function)
            else:
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
                if (self.input_set_files == True):
                    slice_object['path'] = input_slice
                    slice_object['binary'] = self.__file_writer(input_slice)
                if (self.range_object_input == False):
                    if self.node_js == False:
                        if self.pickle_input_set == True:
                            input_slice_encoded = self.__pickle_jar(input_slice, self.compress_input_set)
                        elif self.encode_input_set == True:
                            input_slice_encoded = self.__input_encoder(input_slice)
                        else:
                            input_slice_encoded = input_slice
                    else:
                        input_slice_encoded = input_slice
                    slice_object['data'] = input_slice_encoded

                input_set_encoded.append(slice_object)

        job_input = []
        for i in range(self.multiplier):
            job_input.extend(input_set_encoded)

        if self.shuffle == True:
            random.shuffle(job_input)

        if self.node_js == False and self.debug == False:
            self.events['console'] = False

        dcp_parameters = {
            'dcp_data': job_input,
            'dcp_wrapper': self.python_wrapper,
            'dcp_debug': self.debug,
            'dcp_events': self.events,
            'dcp_kvin': self.kvin,
            'dcp_local': self.local_cores,
            'dcp_multiplier': self.multiplier,
            'dcp_node_js': self.node_js,
            'dcp_show_timings': self.show_timings,
            'dcp_remote_flags': self.remote,
            'dcp_remote_storage_location': self.remote_storage_location,
            'dcp_remote_storage_params': self.remote_storage_params,
        }

        job_parameters = {
            'job_collate': self.collate_results,
            'job_debug': self.debug,
            'job_estimation': self.estimation_slices,
            'job_greedy': self.greedy_estimation,
            'job_groups': self.compute_groups,
            'job_public': self.public,
            'job_requirements': self.requirements,
        }

        worker_parameters = {
            'slice_workload': {
                'workload_function': work_function_encoded,
                'workload_arguments': work_arguments_encoded,
                'workload_named_arguments': work_keyword_arguments_encoded,
            },
            'python_modules': work_imports_encoded,
            'python_imports': self.python_imports,
            'python_packages': self.require_path,
            'python_files': {
                'files_path': self.files_path,
                'files_data': self.files_data,
            },
            'python_functions': {
                'init': self.python_init,
                'compute': self.python_compute,
            },
        }

        worker_config_flags = {
            'pickle': {
                'function': self.pickle_work_function,
                'arguments': self.pickle_work_arguments,
                'input': self.pickle_input_set,
                'output': self.pickle_output_set,
            },
            'encode': {
                'function': self.encode_work_function,
                'arguments': self.encode_work_arguments,
                'input': self.encode_input_set,
                'output': self.encode_output_set,
            },
            'compress': {
                'function': self.compress_work_function,
                'arguments': self.compress_work_arguments,
                'input': self.compress_input_set,
                'output': self.compress_output_set,
            },
            'files': {
                'input': self.input_set_files,
            },
            'pyodide': {
                'wheels': self.pyodide_wheels,
            },
            'cloudpickle': self.cloudpickle,
        }

        run_parameters = {
            #'client_parameters': {
            #    'bifrost': dcp_parameters,
            #    'job_handle': job_parameters,
            #}
            'dcp_parameters': dcp_parameters,
            'job_parameters': job_parameters,
            'worker_parameters': worker_parameters,
            'worker_config_flags': worker_config_flags,
        }

        node_output = node.run(self.python_deploy, run_parameters)

        try:
          self.id = node_output['jobId']
        except:
          print('Warning : Job ID not found.')

        result_set = node_output['jobOutput']

        for result_index, result_slice in enumerate(result_set):
            if self.node_js == False:
                if self.pickle_output_set == True:
                    result_slice = self.__unpickle_jar( result_slice, self.compress_output_set )
                elif self.encode_output_set == True:
                    result_slice = self.__output_decoder(result_slice)
            result_set[result_index] = result_slice

        self.result_set = result_set
        
        if self.new_context == True:
            node.clear()

        return result_set

    def on(self, event_name, event_function):
        self.events[event_name] = event_function

    def add_listener(self, event_name, event_function):
        on(self, event_name, event_function)

    def add_event_listener(self, event_name, event_function):
        on(self, event_name, event_function)

    def requires(self, *package_arguments):
        # adds dcp pyodide packages to be required in the worker function
        for package_element in package_arguments:
            element_type = type(package_element)
            if (element_type is str):
                self.require_path.append(package_element)
            elif (element_type is list or element_type is tuple):
                self.requires(*package_element)
            else:
                print('Warning: unsupported format for Job.requires:', element_type)

    def imports(self, *module_arguments):
        # adds local python modules to be imported in the worker function
        for module_element in module_arguments:
            element_type = type(module_element)
            if (element_type is str):
                self.python_imports.append(module_element)
            elif (element_type is list or element_type is tuple):
                self.imports(*module_element)
            else:
                print('Warning: unsupported format for Job.imports:', element_type)

    def files(self, *files_arguments):
        # adds files to be made available in the worker virtual file system
        for file_element in files_arguments:
            element_type = type(file_element)
            # TODO: add support for user-submitted data buffers
            # TODO: add support for user-submitted byte strings
            # TODO: add support for user-submitted remote file urls
            if (element_type is str):
                self.files_path.append(file_element)
                file_data = self.__file_writer(file_element)
                self.files_data[file_element] = file_data
            elif (element_type is list or element_type is tuple):
                self.files(*file_element)
            else:
                print('Warning: unsupported format for Job.files:', element_type)

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

