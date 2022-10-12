# MODULES

# local modules
from .ReadWriteLock import ReadWriteLock
from .NodeSTDProc import NodeSTDProc
from .Node import Node

# PROGRAM

#Simple python global read write lock
NODE_LOCK       = ReadWriteLock()
#variable that is being locked
NODE_IS_RUNNING = False

