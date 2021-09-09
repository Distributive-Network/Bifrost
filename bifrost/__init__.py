# MODULES

# python standard library
import atexit
import os
import signal
import sys
import warnings

# pypi modules
import posix_ipc

# local modules
from .py_ced import CrazyEddieDrive
from .py_nodejs import Npm, Node
from .notebook import BifrostMagics

# PROGRAM

npm = Npm()
node = Node()

memName = node.vs.SHARED_MEMORY_NAME

def isnotebook():
    """
    A function that checks to see if we are in a notebook or not.
    This is necessary so that we know whether ipython specific functions
    are available or not. If they are available, we'd like to use them.
    """
    try:
        shell = get_ipython().__class__.__name__
    except:
        return False      # Probably standard Python interpreter
    else:
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or qtconsole
        elif shell == 'Shell':
            return True
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (Unknown ipython kernel....)

if isnotebook():

    # ipython modules
    from IPython.core.error import TryNext
    from IPython.display import display, HTML

    def shutdown_hook(ipython):
        '''
        This is a hook that will execute when ipython closes.
        '''
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

#When python exists please do the following
@atexit.register
def onEnd():

    #Clean up everything.... Include shm file and mmap stuff.
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

#TODO: restructure Bifrost as class
#TODO: restructure CrazyEddieDrive as subclass of Bifrost

# set up our DCP bridge interface
ced = CrazyEddieDrive(npm, node, 'https://scheduler.distributed.computer')

dcp_install = ced.dcp_install
dcp_wallet = ced.dcp_wallet
dcp_deploy = ced.dcp_deploy
