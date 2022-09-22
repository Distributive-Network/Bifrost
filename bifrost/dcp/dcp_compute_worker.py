# input serialization
import codecs
import pickle
import zlib

# js proxy module for dcp progress calls
from js import dcp

config_flags = worker_config_flags.to_py()

# pickle selection

if config_flags['cloudpickle'] == True:
  import cloudpickle
  pickle_in = cloudpickle.loads
  pickle_out = cloudpickle.dumps
else:
  pickle_in = pickle.loads
  pickle_out = pickle.dumps

# serialization functions

def cereal_bowl(target_name, target_data):

  if config_flags['encode'][target_name] == True:
    target_data = codecs.decode( target_data.encode(), 'base64' )

  if config_flags['compress'][target_name] == True:
    target_data = zlib.decompress( target_data )

  if config_flags['pickle'][target_name] == True:
    target_data = pickle_in( target_data )
  else:
    # degenerate pythonic EAFP pattern; consider purging in favour of JsProxy type check
    try:
      target_data = target_data.to_py()
    except AttributeError:
      target_data = target_data

  dcp.progress()

  return target_data

def cereal_box(target_name, target_data):

  if config_flags['pickle'][target_name] == True:
    target_data = pickle_out(target_data)

  if config_flags['compress'][target_name] == True:
    target_data = zlib.compress( target_data )

  if config_flags['encode'][target_name] == True:
    target_data = codecs.encode( target_data, 'base64' ).decode()

  return target_data

# deserialization of compute elements

compute_args = cereal_bowl('arguments', input_parameters)
compute_kargs = cereal_bowl('arguments', input_keyword_parameters)
compute_data = cereal_bowl('input', input_data)

if config_flags['pickle']['function'] == True:
  compute_function = cereal_bowl('function', input_function)
else:
  compute_function = locals()[input_function]

# run compute function with slice input and args

output_data = compute_function( compute_data, *compute_args, **compute_kargs )

# serialization of final output

output_data = cereal_box('output', output_data)

