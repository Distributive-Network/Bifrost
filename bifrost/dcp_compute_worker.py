# input serialization
import codecs
import cloudpickle

# decode and unpickle secondary arguments to compute function
parameters_decoded = codecs.decode( input_parameters.encode(), 'base64' )
parameters_unpickled = cloudpickle.loads( parameters_decoded )

# decode and unpickle primary argument to compute function
data_decoded = codecs.decode( input_data.encode(), 'base64' )
data_unpickled = cloudpickle.loads( data_decoded )

compute_function = locals()[input_function]

output_data = compute_function( data_unpickled, **parameters_unpickled )

output_data = cloudpickle.dumps( output_data )
output_data = codecs.encode( output_data, 'base64' ).decode()

