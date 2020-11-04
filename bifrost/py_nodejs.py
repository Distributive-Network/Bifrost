from .py_storage import *
from .ReadWriteLock import ReadWriteLock
import time, posix_ipc
import numpy as np
import os, sys, socket
import subprocess, signal
from threading import Thread, Event, Lock
from subprocess import call, Popen, PIPE
import atexit


NODE_LOCK       = ReadWriteLock()
NODE_IS_RUNNING = False

class Npm():
    def __init__(self, cwd = os.getcwd()):
        self.cwd = cwd
        if not (os.path.exists(cwd + '/node_modules/npy-js') and os.path.exists(cwd + '/node_modules/mmap.js')):
            self.run(['npm', 'init', '--yes'])
            self.run(['npm', 'install', 
                      'git+https://github.com/Kings-Distributed-Systems/npy-js.git', 'git+https://github.com/bungabear/mmap.js', 'nodeshm'])

    def run(self, cmd):
        process = Popen(cmd, cwd = self.cwd, stdout = subprocess.PIPE)
        while True:
            output = process.stdout.readline().decode('utf-8')
            if output == '' and process.poll() is not None:
                break
            if output:
                print(output.strip())
        returnCode = process.poll()
        return returnCode

    def install(self,*args):
        self.run(['npm', 'install', *args])

    def uninstall(self, *args):
        self.run(['npm', 'uninstall', *args])

    def list_modules(self):
        self.run(['npm', 'list'])


class NodeSTDProc(Thread):
    def __init__(self, process):
        super(NodeSTDProc, self).__init__()
        self.process     = process
        self._stop_event = Event()
        self.daemon      = True
        self.start()

    def stop(self):
        self._stop_event.set()

    def run(self):
        while not self._stop_event.is_set():
            global NODE_IS_RUNNING
            global NODE_LOCK
            output = self.process.stdout.readline().decode('utf-8')
            if self.process.poll() is not None:
                NODE_LOCK.acquire_write()
                NODE_IS_RUNNING = False
                NODE_LOCK.release_write()
                break
            if output == '':
                continue
            if output:
                try:
                    output_json = json.loads(output)
                    if output_json['type'] == 'done':
                        NODE_LOCK.acquire_write()
                        NODE_IS_RUNNING = False
                        NODE_LOCK.release_write()
                    continue
                except Exception as e:
                    if (output and len(output.strip()) > 0):
                        print(output.strip())
                    continue 





class Node():
    def __init__(self, cwd= os.getcwd()):
        self.cwd = cwd
        self.serializer_custom_funcs = {}
        self.deserializer_custom_funcs = {}
        self.replFile = os.path.dirname(os.path.realpath(__file__)) + '/main.js'
        self.vs = VariableSync()
        self.init_process()


    def init_process(self):
        env = os.environ
        env["NODE_PATH"] = self.cwd + '/node_modules'
        self.process = Popen(['node',
                              '--max-old-space-size=10000',
                              self.replFile,
                              self.vs.SHARED_MEMORY_NAME], cwd=self.cwd,stdin=subprocess.PIPE,
                              env=env,
                              stdout=subprocess.PIPE)
        self.nstdproc = NodeSTDProc(self.process)

    def register_custom_serializer(self, func, var_type):
        if var_type is not str:
            var_type = str(var_type)
        self.serializer_custom_funcs[var_type] = func
        return

    def register_custom_deserializer(self, func, var_type):
        if var_type is not str:
            var_type = str(var_type)
        self.deserializer_custom_funcs[var_type] = func

    def run(self, script, vars = {}, timeout=None):
        self.vs.syncto(vars, self.serializer_custom_funcs, warn=False)
        global NODE_IS_RUNNING
        global NODE_LOCK
        NODE_LOCK.acquire_write()
        NODE_IS_RUNNING = True
        NODE_LOCK.release_write()
        retCode = self.write(script)

        if retCode < 0:
            print("Could not run script")
            return

        flag = NODE_IS_RUNNING
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
        try:
            string_to_send = json.dumps(
                {'script': s}
            )
            self.process.stdin.write(string_to_send.encode('utf-8'))
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
            os.kill(self.process.pid, signal.SIGSTOP)
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


npm = Npm()
node = Node()
memName = node.vs.SHARED_MEMORY_NAME

@atexit.register
def onEnd():
    global memName
    global node

    try:
        if hasattr(node, 'process'):
            os.kill(node.process.pid, signal.SIGSTOP)
    except:
        print("Could not kill process. May already be dead.")
    try:
        if hasattr(node, 'nstdproc'):
            node.nstdproc.stop()
    except:
        print("Could not stop nstdproc. May already be dead.")

    posix_ipc.unlink_shared_memory(memName)
    print("Memory map has been destroyed")



