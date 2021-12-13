def dcp_init_worker():

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

    for module_name in input_imports:

        module_spec = importlib.util.find_spec(module_name)

        if module_spec is None:
            module_runtime(module_name, input_modules[module_name])

def dcp_compute_worker():

    # input serialization
    import codecs
    import cloudpickle

    function_decoded = codecs.decode( input_function.encode(), 'base64' )
    function_unpickled = cloudpickle.loads( function_decoded )

    parameters_decoded = codecs.decode( input_parameters.encode(), 'base64' )
    parameters_unpickled = cloudpickle.loads( parameters_decoded )

    data_decoded = codecs.decode( input_data.encode(), 'base64' )
    data_unpickled = cloudpickle.loads( data_decoded )

    output_data = function_unpickled( data_unpickled, **parameters_unpickled )

    output_data_pickled = cloudpickle.dumps( output_data )
    output_data_encoded = codecs.encode( output_data_pickled, 'base64' ).decode()

    return output_data_encoded

