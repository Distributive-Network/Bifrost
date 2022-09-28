# MODULES

# python standard library
import base64
import json
import math
import mmap
import os
import sys
import uuid
from io import BytesIO
from tempfile import TemporaryFile

# pypi modules
import numpy as np
import xxhash

# local modules
from .py_utils import is_windows, is_darwin, is_notebook, has_mp_shared, is_colab

# PROGRAM

class VariableSync():
    '''
    Helper library to synchronize variables between python and node
    '''
    def __init__(self):
        self.variables = []
        self.windows = is_windows()
        self.darwin = is_darwin()
        self.notebook = is_notebook()
        self.mp_shared = has_mp_shared()
        self.colab = is_colab()
        self.shmmap = os.path.exists(os.getcwd() + '/node_modules/shmmap')

        if os.name == 'posix':
            if self.darwin or self.colab or not self.shmmap:
                self.shared = 'fs'
            elif self.mp_shared:
                self.shared = 'multiprocessing'
            else:
                self.shared = 'posix_ipc'
        else:
            self.shared = 'fs'

        #max size experimentally was 3/4 of 1gb
        #Likely some problem the mmap/shm_open library used
        self.size = int(math.floor( 0.75 *(1024*1024*1024) ))
        #Set some arbitrary name for the file

        if self.shared == 'fs':
            self.SHARED_MEMORY_NAME = os.getcwd() + "/bifrost_shared_memory_" + str(uuid.uuid4())
            with open(self.SHARED_MEMORY_NAME, "w+b") as self.file_obj:
                #truncate the shared memory so that we are not mapping to an empty file
                self.file_obj.truncate( self.size )
                #map the file to memory
                self.file_obj.flush()
                self.mapFile = mmap.mmap(self.file_obj.fileno(), self.size, access=mmap.ACCESS_WRITE)
        elif self.shared == 'posix_ipc':
            self.SHARED_MEMORY_NAME = "/bifrost_shared_memory_" + str(uuid.uuid4())
            import posix_ipc
            self.memory = posix_ipc.SharedMemory(
                self.SHARED_MEMORY_NAME,
                flags=posix_ipc.O_CREX,
                size=self.size
            )
            self.mapFile = mmap.mmap(self.memory.fd, self.memory.size)
            self.memory.close_fd()
        else:
            self.SHARED_MEMORY_NAME = "bifrost_shared_memory_" + str(uuid.uuid4())
            from multiprocessing.shared_memory import SharedMemory
            self.memory = SharedMemory(
              name=self.SHARED_MEMORY_NAME,
              create=True,
              size=self.size,
            )
            self.mapFile = mmap.mmap(self.memory._fd, self.size, access=mmap.ACCESS_WRITE)
            self.memory.close()
        self.clearCache()

    def __del__(self):
        '''
        If this variable is deleted, we should manage it appropriately
        '''
        try:
            if sys.meta_path:
                self.mapFile.close()
        except:
            print("Could not close shared memory. Process may be ending.")

        try:
            if sys.meta_path:
                if self.shared == 'fs':
                    os.remove(self.SHARED_MEMORY_NAME)
                elif self.shared == 'posix_ipc':
                    posix_ipc.unlink_shared_memory(self.SHARED_MEMORY_NAME)
                else:
                    self.memory.unlink()
        except:
            print("Could not unlink shared memory. Process may be ending.")

        return

    def setCache(self, key, hsh):
        '''
        Set the cache using a hash.
        '''
        self.cache[key] = hsh

    def inCache(self, key, val, var_type):
        '''
        Check if variable is in cache by hashing 
        '''
        hsh = ''
        if var_type == np.ndarray:
            arr_bytes = bytes(val.data)
            hsh = xxhash.xxh32(arr_bytes).hexdigest() + str(val.shape)
        else:
            hsh = xxhash.xxh32( json.dumps(val).encode('utf8') ).hexdigest()
        if key in self.cache and hsh == self.cache[key]:
            return True, hsh
        return False, hsh


    def clearCache(self):
        '''Empty the cache'''
        self.cache = {}

    def parse_variables(self, final_output, var_dict, keys, custom_funcs, warn=False):
        '''
        Parse variables into a JSON serializable dictionary.
        '''

        for var_name in keys:
            var = var_dict[var_name]
            var_type = type(var)
            # XXX: python caching needs to be safe for recursion before it is reactivated
            '''
            try:
                b, hsh = self.inCache(var_name, var, var_type)
                if b:
                    continue
                else:
                    self.setCache(var_name, hsh)
            except Exception as e:
                # TODO: this control flow pattern needs to be purged
                pass
            '''
            if var_type == np.ndarray:
                #Numpy is a special case and requires some managing to get data into a buffer
                outBytes = BytesIO()
                np.save(outBytes, var, allow_pickle=False)
                outBytes.seek(0)
                out_bytes = outBytes.read()
                out_b64 =  base64.b64encode(out_bytes)
                out_string = out_b64.decode('ascii')
                final_output[var_name] = {
                    'type': 'numpy',
                    'data': out_string
                }
            elif var_type == str:
                final_output[var_name] = var
            elif var_type == int or var_type==float:
                if var == math.inf:
                    final_output[var_name] = {
                        'type': 'infinity'
                    }
                else:
                    final_output[var_name] = var
            elif var_type == bool:
                final_output[var_name] = var
            elif var_type == dict:
                final_output[var_name] = self.parse_variables({}, var, var.keys(), custom_funcs, warn)
            elif var_type == list:
                final_output[var_name] = self.parse_variables([None]*len(var), var, range(len(var)), custom_funcs, warn)
            elif var_type == type(None):
                final_output[var_name] = None
            else:
                if (custom_funcs is not None and str(var_type) in custom_funcs):
                    final_output[var_name] = custom_funcs[var_type](var)
                    assert final_output[var_name] == str, "custom function for type " + var_type + " must return ascii string."
                else:
                    if warn:
                        print("variable type not serializeable; skipping.\n - var type, var name :", var_type, var_name)
                    continue

        return final_output

    def unparse_variables(self, final_output, var_dict, keys, custom_funcs, warn=False):
        '''
        Reverse work of parse_variables by deserializaing JSON
        '''
        for var_name in keys:
            var = var_dict[var_name]
            var_type = type(var)

            if var_type == dict:
                if 'type' in var and 'data' in var:
                    if(var['type'] == 'numpy'):
                        data = var['data']
                        data_bytes = base64.b64decode(data)
                        load_bytes = BytesIO(data_bytes)
                        loaded_np = np.load(load_bytes, allow_pickle=False)
                        final_output[var_name] = loaded_np
                    else:
                        data = var['data']
                        try:
                            final_output[var_name] = custom_funcs[var['type']](data)
                        except Exception as e:
                            if warn:
                                print(e)
                else:
                    final_output[var_name] = self.unparse_variables({}, var, var.keys(), custom_funcs, warn)
            elif var_type == list:
                final_output[var_name] = self.unparse_variables([None]*len(var), var, range(len(var)), custom_funcs, warn)
            else:
                final_output[var_name] = var_dict[var_name]
        return final_output


    def syncto(self, var_dict, custom_funcs=None, warn = False):
        '''
        Sync our variables into the node context
        '''
        for key in list(var_dict.keys()):
            if key.startswith('_'):
                var_dict.pop(key, None)

        final_output = self.parse_variables({}, var_dict, var_dict.keys(), custom_funcs, warn = warn)
        final_str = json.dumps(final_output) + '\n'
        final_bytes = str.encode(final_str)
        try:
            self.mapFile.seek(0)
            self.mapFile.write(final_bytes)#.ljust(self.size, b'\0'))
        except Exception as e:
            print("Could not write final bytes due to : ")
            print(e)
        return final_output

    def syncfrom(self, custom_funcs=None, warn=False):
        '''
        Sync from the node context into the current python process.
        '''
        self.mapFile.seek(0)
        byte_lines = self.mapFile.readline()
        final_str = byte_lines.decode()

        final_output = json.loads(final_str)
        vars_to_sync = self.unparse_variables({}, final_output, final_output.keys(), custom_funcs, warn=warn)

        return vars_to_sync
