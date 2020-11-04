import math, json, sys, hashlib, posix_ipc, mmap
import xxhash
from tempfile import TemporaryFile
from io import BytesIO
import base64, uuid
import numpy as np


class VariableSync():
    def __init__(self):
        self.variables = []
        self.size = int(math.floor( 0.75 *(1024*1024*1024) ))
        self.SHARED_MEMORY_NAME = "/bifrost_shared_memory" + str(uuid.uuid4())
        self.memory = posix_ipc.SharedMemory(self.SHARED_MEMORY_NAME, posix_ipc.O_CREX,
                                        size=self.size)

        self.mapFile = mmap.mmap(self.memory.fd, self.memory.size)

        self.memory.close_fd()
        self.clearCache()
        print("Memory map has been established")

    def __del__(self):
        try:
            self.mapFile.close()
            posix_ipc.unlink_shared_memory(self.SHARED_MEMORY_NAME)
            print("Memory unlinked!")
        except Exception as e:
            print(str(e))
            print("Could not unlink shared memory for some reason")
        return

    def setCache(self, key, hsh):
        self.cache[key] = hsh

    def inCache(self, key, val, var_type):
        hsh = ''
        if var_type == np.ndarray:
            arr_bytes = bytes(val.data)
            hsh = xxhash.xxh32( arr_bytes ).hexdigest() + str(val.shape)
        else:
            hsh = xxhash.xxh32( JSON.dumps(val).encode('utf8') ).hexdigest()
        if key in self.cache and hsh == self.cache[key]:
            return True, hsh
        return False, hsh


    def clearCache(self):
        self.cache = {}

    def parse_variables(self, var_dict, keys, custom_funcs, warn=False):
        final_output = {}
        for var_name in keys:
            var = var_dict[var_name]
            var_type = type(var)
            try:
                b, hsh = self.inCache(var_name, var, var_type)
                if b:
                    continue
                else:
                    self.setCache(var_name, hsh)
            except Exception as e:
                pass
            if var_type == np.ndarray:
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
                final_output[var_name] = var
            elif var_type == bool:
                final_output[var_name] = var
            elif var_type == dict:
                final_output[var_name] = var
            elif var_type == list:
                final_output[var_name] = var
            elif var_type == type(None):
                final_output[var_name] = None
            else:
                if (custom_funcs is not None and str(var_type) in custom_funcs):
                    final_output[var_name] = custom_funcs[var_type](var)
                    assert final_output[var_name] == str, "custom function for type " + var_type + " must return ascii string."
                else:
                    if warn:
                        print("variable type no serializeable so skipping -  var type, var name :", var_type, var_name)
                    continue

        return final_output

    def unparse_variables(self, var_dict, custom_funcs, warn=False):
        final_output = {}
        for var_name in list(var_dict.keys()):
            var = var_dict[var_name]

            if type(var) == dict:
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
                    final_output[var_name] = var_dict[var_name]
            else:
                final_output[var_name] = var_dict[var_name]
        return final_output


    def syncto(self, var_dict, custom_funcs=None, warn = False):
        for key in list(var_dict.keys()):
            if key.startswith('_'):
                var_dict.pop(key, None)

        final_output = self.parse_variables(var_dict, var_dict.keys(), custom_funcs, warn = warn)
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
        self.mapFile.seek(0)
        byte_lines = self.mapFile.readline()
        final_str = byte_lines.decode()

        final_output = json.loads(final_str)
        vars_to_sync = self.unparse_variables(final_output, custom_funcs, warn=warn)

        return vars_to_sync
