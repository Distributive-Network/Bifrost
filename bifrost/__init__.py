import sys
import os

import warnings

from .py_nodejs import npm, node

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

def shutdown_hook(ipython):
    '''
    This is a hook that will execute when ipython closes.
    '''
    node.cancel(restart=False)
    raise TryNext

if isnotebook():

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
