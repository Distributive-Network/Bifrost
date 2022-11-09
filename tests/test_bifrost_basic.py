import time
import numpy as np
from bifrost import node, npm

def check_equals(in_vals, out_vals):
    """
    Returns true if both values given are equal
    """
    flag = True
    for key in in_vals.keys(): 
        if key in out_vals:
            if isinstance(in_vals[key], dict):
                if (not check_equals(in_vals[key], out_vals[key])):
                    return False
            elif isinstance(in_vals[key], np.ndarray):
                if (not np.isclose(in_vals[key], out_vals[key])):
                    return False
            else:
                if (out_vals[key] != in_vals[key]):
                    print(key, " is not equivalent! problem during serialization")
                    return False
    return flag

def test_simple_serialization():

    vals = {
     'n': 35,
     'm': 128,
     'c': 7.6981928384058191820394,
     'f': True,
     'arr': list(range(20,30, 2))
    }

    for i in range(10):
        out_vals = node.run("""
        var log = (...vals)=>{
          vals = ["FROM JS:" , ...vals];
          console.log(...vals);
        };
        log('n:', n, ' typeof: ', typeof n);
        log('m:', m, ' typeof: ', typeof m);
        log('c:', c, ' typeof: ', typeof c);
        log('f:', f, ' typeof: ', typeof f);
        log('arr', arr, ' typeof: ', typeof arr, ' typeof elem: ', typeof arr[0]); 
        """, vals)
        print("serialization round: ", str(i), end=" - ")
        if (check_equals(vals, out_vals)):
            print("No problem found")
        else:
            raise ValueError("Problem occured during serialization")
            #print("Problem during initial serialization")

def test_numpy_serialization():
    for i in range(5):
        vals = {
            'a': np.random.randn(100, 224,224, 3)
        }
        start = time.time()
        out_vals = node.run("""
        console.log(a.shape, a.typedArray.length);
        """, vals)
        end = time.time()
        print("Took: ", end - start, " seconds")
        assert np.allclose( out_vals['a'], vals['a'] ), "numpy array de/serialization did not work"


def test_simple_serialization_2():
    parameter_space = {
        "activation": ['linear','relu','selu','sigmoid','softmax', 'tanh'],
        "optimizer": ['SGD','Adagrad','Adadelta','Adam','Adamax','RMSprop'],
        "num_layers": [1, 2, 3, 4, 5, 6],
        "num_units": [1, 2, 4, 8, 16, 32],
        "lr": [1, 0.1, 0.01, 0.001, 0.0001, 0.00001],
    }
    vals = {'parameter_space': parameter_space}
    for i in range(5):
        start = time.time()
        out_vals = node.run("""
        console.log(parameter_space);
        """, vals)
        end = time.time()
        print("Took: ", end - start, " seconds")
        assert check_equals(vals, out_vals), "various parameter space de/serialization failed."
