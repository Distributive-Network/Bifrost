import os
import sys

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

def is_windows():
  """
  A function that checks to see if we are in a Windows NT environment.
  Windows handles process management differently, and introduces other
  restrictions to how we share memory and make interprocess commands.
  """
  if os.name == 'nt':
    return True
  else:
    return False

def has_mp_shared():
  """
  A function that checks the current python version number, to check whether
  we are in an environment that supports the multiprocessing.shared_memory
  functionality that was introduced in Python 3.8, allowing easy shmmap calls.
  """
  # TODO: consider attempting to actually import multiprocessing.shared_memory here
  if sys.version_info.major == 3 and sys.version_info.minor >= 8:
    return True
  else:
    return False

