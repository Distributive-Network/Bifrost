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
                'git+https://github.com/Kings-Distributed-Systems/npy-js.git', 'mmap.js', 'nodeshm'])
    
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
            output = self.process.stdout.readline().decode('utf-8')
            if self.process.poll() is not None:
                break
            if output == '':
                continue
            if output:
                try:
                    output_json = json.loads(output)
                    if output_json['type'] == 'done':
                        global NODE_IS_RUNNING
                        global NODE_LOCK
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
        self.replFile = os.path.dirname(os.path.realpath(__file__)) + '/js/main.js'
        self.vs = VariableSync()
        self.init_process()


    def init_process(self):
        self.process = Popen(['node', 
                              '--max-old-space-size=10000', 
                              self.replFile, 
                              self.vs.SHARED_MEMORY_NAME], cwd=self.cwd,stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE)
        self.nstdproc = NodeSTDProc(self.process)

    def run(self, script, timeout=None):
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
        start = time.time()
        while flag:
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
        return 1


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
        
    def cancel(self):
        os.kill(self.process.pid, signal.SIGSTOP)
        self.nstdproc.stop()
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

    if hasattr(node, 'process'):
        os.kill(node.process.pid, signal.SIGSTOP)
    if hasattr(node, 'nstdproc'):
        node.nstdproc.stop()

    posix_ipc.unlink_shared_memory(memName)
    print("Memory map has been destroyed")



# print(os.getcwd())
# print(os.path.dirname(os.path.realpath(__file__)))

# def test():    
#     npm = Npm()
#     node = Node()


#     a = np.random.randn( 30, 28, 28, 1)
#     b = np.random.randn( 10, 28, 28, 1)
#     _temp = np.copy(a)

#     s = time.time()
#     for i in range(5):
#         node.vs.syncto({'a':a, 'b': b}, warn=True)
#     e = time.time()

#     print("Time taken: ", (e-s)/5)
#     out = node.vs.syncfrom()
#     print(np.array_equal(_temp, out['a']))

#     node.vs.syncto({'a': "hello \n world", 'b': b})

#     #print(node.vs.syncfrom())

#     print(b[0][0][0][0])
#     node.run('console.log("Hello world");console.log(a);console.log(b.typedArray[0]);b.typedArray[0] = 1;')

#     print(node.vs.syncfrom()['a'])
#     print(node.vs.syncfrom()['b'][0][0][0][0])
#     # node.run("""
#     # async function main(){
#     #     console.log("Waiting 5 seconds");
#     #     await new Promise((resolve, reject)=>{
#     #         setTimeout(resolve, 5*1000);
#     #     });
#     #     console.log("Done waiting!");
#     # }
#     # main();
#     # """)

# test()