# MODULES

# python standard library
import json
import os
import shutil
import signal
import time
from pathlib import Path
from subprocess import Popen, PIPE

# local modules
from .VariableSync import VariableSync
from .ReadWriteLock import ReadWriteLock
from .NodeSTDProc import NodeSTDProc

# PROGRAM

class Node():
    '''
    This class is a helper class to manage the node process. 
    '''
    def __init__(self, cwd= os.getcwd()):
        self.cwd = cwd
        self.node_exec_path = shutil.which('node')
        self.serializer_custom_funcs = {}
        self.deserializer_custom_funcs = {}
        #the replFile is the main file that preps the node runtime for use with this module
        self.replFile = os.path.dirname(os.path.realpath(__file__)) + '/main.js'
        #The variable synchronization manager
        self.vs = VariableSync()

        self.init_process()

    def init_process(self):
        '''
        Initialize the process by running node with a larger
        max-old-space-size and with all the important information
        regarding the shared memory name and the repl file.
        '''
        env = os.environ
        #make sure to add the current path to the node_path
        env["NODE_PATH"] = self.cwd + '/node_modules'

        #ready the node process
        self.process = Popen(
          [
            self.node_exec_path,
            '--max-old-space-size=32000',
            self.replFile,
            str(self.vs.shared),
            str(self.vs.mp_shared),
            str(self.vs.notebook),
            str(self.vs.windows),
            self.vs.SHARED_MEMORY_NAME
          ],
          cwd = self.cwd,
          stdin = PIPE,
          env = env,
          stdout = PIPE
        )

        #ready the node stdout manager
        self.nstdproc = NodeSTDProc(self.process)

    def register_custom_serializer(self, func, var_type):
        '''
        Register a custom serializer for a particular variable type
        '''
        if var_type is not str:
            var_type = str(var_type)
        self.serializer_custom_funcs[var_type] = func
        return

    def register_custom_deserializer(self, func, var_type):
        '''
        Register a custom deserializer for a particular variable type
        '''
        if var_type is not str:
            var_type = str(var_type)
        self.deserializer_custom_funcs[var_type] = func

    def run_file(self, filename, vars = {}, timeout=None):

        script = Path(filename).read_text()
        
        vars = self.run(script, vars, timeout)

        return vars

    def run(self, script, vars = {}, timeout=None):
        '''
        The main function which runs some node script.

        Will synchronize variables and wait a max of timeout before cancelling job
        run. Note that timeout is default to None and if it is None will never timeout.

        '''

        #synchronize variables first
        self.vs.syncto(vars, self.serializer_custom_funcs, warn=False)

        #get the lock and mark as running.
        global NODE_IS_RUNNING
        global NODE_LOCK
        NODE_LOCK.acquire_write()
        NODE_IS_RUNNING = True
        NODE_LOCK.release_write()
        #Send script to process.
        retCode = self.write(script)

        if retCode < 0:
            print("Could not run script")
            return

        #Keep running until the stdout process marks
        #NODE_IS_RUNNING to False.
        #This is a little dangerous as we need to make sure that NODE_IS_RUNNING
        #Will, at some point, resolve to False.
        flag = NODE_IS_RUNNING
        start = time.time()
        while flag:
            try:
                NODE_LOCK.acquire_read()
                flag = NODE_IS_RUNNING
                NODE_LOCK.release_read()
                if timeout is not None:
                    if (time.time() - start) > timeout:
                        self.cancel()
                        NODE_LOCK.acquire_write()
                        NODE_IS_RUNNING = False
                        NODE_LOCK.release_write()
                        print("Process took longer than " + str(timeout))
            except KeyboardInterrupt:
                self.cancel()
                NODE_LOCK.acquire_write()
                NODE_IS_RUNNING = False
                NODE_LOCK.release_write()
                print("Process was interrupted.")
                raise KeyboardInterrupt
        new_vars = self.vs.syncfrom(self.deserializer_custom_funcs, warn=False)
        for key in new_vars.keys():
            vars[key] = new_vars[key]
        return vars

    def clean_lock(self):
        global NODE_LOCK
        global NODE_IS_RUNNING
        NODE_IS_RUNNING = False
        NODE_LOCK = ReadWriteLock()


    def write(self, s):
        '''
        Helper function to submit node script to node process.
        '''
        try:
            msg_json = json.dumps(
                {'script': s}
            )

            # Each message begins with header from E00000000C to EffffffffC
            # : E in position 0, indicating extended message
            # : C in position 9, indicating concatenated message
            # : hexademical digits from 0 to f in positions 1 to 8, together indicating message total length
            # : message length in header includes Bifrost's JSON wrapping, but does not include header itself

            msg_length_int = len(msg_json)
            msg_length_hex = hex(msg_length_int)
            msg_length_str = str(msg_length_hex)[2:]
            if (len(msg_length_str) > 8 or msg_length_int >= 16**8):
              raise("Script size exceeds Node.js string limit:", str(msg_length_int))
            msg_head = 'E' + msg_length_str.zfill(8) + 'C'
            string_to_send = msg_head + msg_json
            string_encoded = string_to_send.encode('utf-8')
            self.process.stdin.write(string_encoded)
            self.process.stdin.flush()
        except Exception as e:
            global NODE_IS_RUNNING
            global NODE_LOCK
            if 'Broken pipe' in str(e):
                self.cancel()
                print("Pipe broke and was restarted")
            else:
                self.cancel()
                print("Pipe died for some reason: ")
                print(str(e))
            NODE_LOCK.acquire_write()
            NODE_IS_RUNNING = False
            NODE_LOCK.release_write()
            return -1
        return 1

    def cancel(self, restart=True):
        try:
            os.kill(self.process.pid, signal.SIGTERM)
            self.nstdproc.stop()
        except Exception as e:
            print(e)
        try:
            self.nstdproc.stop()
        except Exception as e:
            print(e)
        if restart:
            self.init_process()

    def clear(self):
        self.cancel()

