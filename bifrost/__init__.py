import sys, os

def isnotebook():
    """
    A function that checks to see if we are in a notebook or not.
    This is necessary so that we knowe whether ipython specific functions
    are available or not. If they are available, we'd like to use them.

    """
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or qtconsole
        elif shell == 'Shell':
            return True
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (Unknown ipython kernel....)
    except NameError:
        return False      # Probably standard Python interpreter


if isnotebook():
    from .notebook import npm, node
else:
    from .py_nodejs import npm, node

from .Dcp import Dcp
dcp = Dcp()
