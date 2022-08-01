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

        # additional job properties
        self.collate_results = True
        self.compute_groups = [{'opaqueId':'WHhetL7mj1w1mw1XV6dxyC','id':1,'joinKey':'public','joinSecret':''}]
        self.debug = False
        self.estimation_slices = 3
        self.greedy_estimation = False
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
        self.encode_work_arguments = False
        self.encode_input_set = False
        self.encode_output_set = False
        self.compress_work_function = True
        self.compress_work_arguments = True
        self.compress_input_set = True
        self.compress_output_set = True
        self.new_context = False # clears the nodejs stream after every job if true
        self.kvin = False # uses the kvin serialization library to decode job results
        self.colab_pickling = False # use non-cloud pickling for colab deployment

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

        if hasattr(cloudpickle, 'register_pickle_by_value'):
            cloudpickle.register_pickle_by_value(bifrost)

        if is_colab():
            data_pickled = pickle.dumps( input_data, protocol=4 )
        else:
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
            if is_colab():
                data_unpickled = pickle.loads( data_decompressed )
            else:
                data_unpickled = cloudpickle.loads( data_decompressed )
        else:
            if is_colab():
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

    def __dcp_run(self):

        from bifrost import node

        if is_colab():
            self.colab_pickling = True
            self.pickle_work_function = False

        if self.node_js == True:
            work_arguments_encoded = False # self.__input_encoder(self.work_arguments)

            node.run("""
            globalThis.nodeSharedArguments = [];
            """)

            for argument_index in range(len(self.work_arguments)):
                shared_argument = self.work_arguments[argument_index]
                node.run("""
                nodeSharedArguments.push( sharedArgument );
                """, { 'sharedArgument': shared_argument })

            work_function_encoded = self.work_function # TODO: adapt __function_writer for Node.js files
            work_imports_encoded = {}
        else:
            if self.pickle_work_arguments == True:
                work_arguments_encoded = self.__pickle_jar(self.work_arguments, self.compress_work_arguments)
            elif self.encode_work_arguments == True:
                work_arguments_encoded = self.__input_encoder(self.work_arguments)
            else:
                work_arguments_encoded = self.work_arguments
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

        run_parameters = {
            'deploy_function': self.python_wrapper,
            'dcp_data': job_input,
            'dcp_parameters': work_arguments_encoded,
            'dcp_function': work_function_encoded,
            'dcp_multiplier': self.multiplier,
            'dcp_local': self.local_cores,
            'dcp_collate': self.collate_results,
            'dcp_estimation': self.estimation_slices,
            'dcp_greedy': self.greedy_estimation,
            'dcp_groups': self.compute_groups,
            'dcp_public': self.public,
            'dcp_requirements': self.requirements,
            'dcp_debug': self.debug,
            'dcp_node_js': self.node_js,
            'dcp_events': self.events,
            'dcp_kvin': self.kvin,
            'dcp_remote_flags': self.remote,
            'dcp_remote_storage_location': self.remote_storage_location,
            'dcp_remote_storage_params': self.remote_storage_params,
            'python_packages': self.require_path,
            'python_modules': work_imports_encoded,
            'python_imports': self.python_imports,
            'python_init_worker': self.python_init,
            'python_compute_worker': self.python_compute,
            'python_pickle_function': self.pickle_work_function,
            'python_pickle_arguments': self.pickle_work_arguments,
            'python_pickle_input': self.pickle_input_set,
            'python_pickle_output': self.pickle_output_set,
            'python_encode_arguments': self.encode_work_arguments,
            'python_encode_input': self.encode_input_set,
            'python_encode_output': self.encode_output_set,
            'python_compress_function': self.compress_work_function,
            'python_compress_arguments': self.compress_work_arguments,
            'python_compress_input': self.compress_input_set,
            'python_compress_output': self.compress_output_set,
            'python_colab_pickling': self.colab_pickling,
        }

        node_output = node.run(self.python_deploy, run_parameters)

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

