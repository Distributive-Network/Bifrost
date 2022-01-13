# PROGRAM

def is_notebook():
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
            return False  # Other type (Unknown ipython kernel....) # TODO: investigate: should this default in the other direction?

