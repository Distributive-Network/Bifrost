# input serialization
import codecs
import cloudpickle

if (pickle_arguments == True):
  # decode and unpickle secondary arguments to compute function
  parameters_decoded = codecs.decode( input_parameters.encode(), 'base64' )
  parameters_unpickled = cloudpickle.loads( parameters_decoded )
else:
  parameters_unpickled = input_parameters

if (pickle_input == True):
  # decode and unpickle primary argument to compute function
  data_decoded = codecs.decode( input_data.encode(), 'base64' )
  data_unpickled = cloudpickle.loads( data_decoded )
else:
  data_unpickled = input_data

if (pickle_function == True):
  # decode and unpickle compute_function from input cloudpickle object
  function_decoded = codecs.decode( input_function.encode(), 'base64' )
  compute_function = cloudpickle.loads( function_decoded )
else:
  # assign compute_function to previously evaluated function definition
  compute_function = locals()[input_function]

output_data_raw = compute_function( data_unpickled, **parameters_unpickled )

if (pickle_output == True):
  output_data_pickled = cloudpickle.dumps( output_data_raw )
  output_data = codecs.encode( output_data_pickled, 'base64' ).decode()
else:
  output_data = output_data_raw

