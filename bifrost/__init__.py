# MODULES

# python standard library
import atexit
import os
import signal
import warnings

# local modules
from .py_utils import is_notebook
from .py_nodejs import Npm, Node
from .Dcp import Dcp

# PROGRAM

npm = Npm()
node = Node()

if is_notebook():

    # ipython modules
    from IPython.core.error import TryNext
    from IPython.display import display, HTML

    # local modules
    from .notebook import BifrostMagics

    def shutdown_hook(ipython):
        '''
        This is a hook that will execute when ipython closes.
        '''
        global node
        node.cancel(restart=False)
        raise TryNext

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            ip = get_ipython()
            magics = BifrostMagics(ip, node)
            ip.register_magics(magics)
            ip.set_hook('shutdown_hook', shutdown_hook)
    except Exception as e:
        print(e)
        raise EnvironmentError("Environment is not as was expected")

#When python exits please do the following
@atexit.register
def onEnd():
    global node

    
    #Clean up everything.... Include shm file and mmap stuff.
    try:
        if hasattr(node, 'process'):
            os.kill(node.process.pid, signal.SIGTERM)
    except:
        print("Could not kill process. May already be dead.")
    try:
        if hasattr(node, 'nstdproc'):
            node.nstdproc.stop()
    except:
        print("Could not stop nstdproc. May already be dead.")
    try:
        node.vs.mapFile.close()
    except:
        print("Could not close shared memory. May already be dead.")
    try:
        if node.vs.shared == 'fs':
            os.remove(node.vs.SHARED_MEMORY_NAME)
        elif node.vs.shared == 'posix_ipc':
            import posix_ipc
            posix_ipc.unlink_shared_memory(node.vs.SHARED_MEMORY_NAME)
        else:
            node.vs.memory.unlink()
    except:
        print("Could not unlink shared memory. May already be dead.")

# set up our DCP bridge interface
dcp = Dcp()

