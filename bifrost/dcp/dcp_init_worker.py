# reset recursion limit
import sys
sys.setrecursionlimit(20000)

# suppress warnings
import warnings
warnings.filterwarnings('ignore')

# custom module loading
import sys
import codecs
import importlib.abc, importlib.util

class StringLoader(importlib.abc.SourceLoader):

    def __init__(self, data):
        self.data = data

    def get_source(self, fullname):
        return self.data
    
    def get_data(self, path):
        return codecs.decode( self.data.encode(), 'base64' )
    
    def get_filename(self, fullname):
        return './' + fullname + '.py'

def module_runtime(module_name, module_encoded):

    module_loader = StringLoader(module_encoded)

    module_spec = importlib.util.spec_from_loader(module_name, module_loader, origin='built-in')
    module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_name] = module
    module_spec.loader.exec_module(module)

# python proxy for js input arrays
py_input_imports = input_imports.to_py()
py_input_modules = input_modules.to_py()

for module_name in py_input_imports:

    module_spec = importlib.util.find_spec(module_name)

    if module_spec is None:
        module_runtime(module_name, py_input_modules[module_name])

# python proxy for js encoded file binaries
py_input_files_path = input_files_path.to_py()
py_input_files_path = input_files_data.to_py()

for file_path in py_input_files_path:

    file_data = py_input_files_path[file_path]

    file_bytes = base64.b64decode( file_data, 'base64' )

    with open(file_path, 'wb') as file_handle:
        file_handle.write(file_bytes)

