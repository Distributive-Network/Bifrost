from .py_nodejs import node, npm
import warnings
from IPython.core.magic import (Magics, magics_class, cell_magic)
from IPython.display import display, HTML
from IPython.core.error import TryNext

@magics_class
class BifrostMagics(Magics):
    def __init__(self, shell, node):
        super(BifrostMagics,self).__init__(shell=shell)
        self.shell = shell
        self._node = node
    
    @cell_magic
    def node(self, line, cell):
        #look at get_ipython().user_ns <- it returns a dict of namespace in user space
        self.shell.user_ns = self._node.run(cell, vars = self.shell.user_ns)
        return


def shutdown_hook(ipython):
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