# MODULES

# ipython modules
from IPython.core.magic import (Magics, magics_class, cell_magic, line_magic)

# PROGRAM

@magics_class
class BifrostMagics(Magics):

    RESERVED = ['true', 'false', 'self', 'this', 'In', 'Out']

    '''
    The BifrostMagics Class is responsible for managing bifrost when called
    using one of the ipython magics. That is to say `%%node`, `%%run_node`
    are managed using this class.
    '''
    def __init__(self, shell, node):
        super(BifrostMagics,self).__init__(shell=shell)
        self.shell = shell
        self._node = node

    def bifrost_sync(self, element):
        '''
        The bifrost_sync function syncs variables between python and node,
        executes the contents of the line or cell in the node process and
        syncs the python and node process once more.
        '''
        #look at get_ipython().user_ns <- it returns a dict of namespace in user space
        vars_to_sync = { k: self.shell.user_ns[k] for k in self.shell.user_ns.keys() if not k.startswith('_') and k not in self.RESERVED }
        try:
            vars_to_sync = self._node.run(element, vars = vars_to_sync)
        except KeyboardInterrupt:
            return 
        for key in vars_to_sync.keys():
            self.shell.user_ns[key] = vars_to_sync[key]
        return

    @cell_magic
    def node(self, line, cell):
        '''
        `%%node` is a cell magic that sync variables between python and node
        '''
        self.bifrost_sync(cell)
        return

    @line_magic
    def run_node(self, line):
        '''
        `%%run_node` is a line magic that sync variables between python and node
        '''
        self.bifrost_sync(line)
        return
