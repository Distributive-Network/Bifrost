# MODULES

# python standard library
import json
from subprocess import Popen, PIPE
from threading import Thread, Event

# PROGRAM

class NodeSTDProc(Thread):
    '''
    Helper class that is run in another thread from the main thread.
    This runs in the background collecting information from the node
    process and deciding how to manage it.

    In particular, this manages stdout and completion of node process
    execution.
    '''
    def __init__(self, process):
        super(NodeSTDProc, self).__init__()
        self.process     = process
        self._stop_event = Event()
        self.daemon      = True
        self.start()

    def stop(self):
        '''
        Stop this thread.
        '''
        self._stop_event.set()

    def run(self):
        '''Main loop for the thread.'''
        while not self._stop_event.is_set():

            global NODE_IS_RUNNING
            global NODE_LOCK
            output = self.process.stdout.readline().decode('utf-8')
            #if process.poll() returns something,
            #this means that the process has ended. End this thread too.
            if self.process.poll() is not None:
                NODE_LOCK.acquire_write()
                NODE_IS_RUNNING = False
                NODE_LOCK.release_write()
                break
            #if our output is an empty string, do nothing.
            if output == '':
                continue
            #'if our output is not an empty string, try processing it.
            if output:
                try:
                    #if our output is json serializeable, check if
                    #it has type=== done before setting NODE_IS_RUNNING to false
                    output_json = json.loads(output)
                    if output_json['type'] == 'done':
                        NODE_LOCK.acquire_write()
                        NODE_IS_RUNNING = False
                        NODE_LOCK.release_write()
                    else:
                        #otherwise print out the json
                        if (output and len(output.strip()) > 0):
                            print(output.strip())
                    continue
                except Exception as e:
                    #otherwise print out the json
                    if (output and len(output.strip()) > 0):
                        print(output.strip())
                    continue

