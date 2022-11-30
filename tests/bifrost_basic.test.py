import time
import numpy as np
from bifrost import node, npm

def check_equals(in_vals, out_vals):
    """
    Returns true if both values given are equal
    """
    for key in in_vals.keys(): 
        if key in out_vals:
            if isinstance(in_vals[key], dict):
                if (not check_equals(in_vals[key], out_vals[key])):
                    return False
            elif isinstance(in_vals[key], np.ndarray):
                if (not check_equals(dict(enumerate(in_vals[key].flatten(), 1)), dict(enumerate(out_vals[key].flatten(), 1)))):
                    return False
            elif isinstance(in_vals[key], list):
                if (not check_equals(dict(enumerate(in_vals[key])), dict(enumerate(out_vals[key])))):
                    return False
            else:
                if (out_vals[key] != in_vals[key]):
                    print(key, " is not equivalent! problem during serialization")
                    return False
    return True


vals = {
 'n': 35,
 'm': 128,
 'c': 7.6981928384058191820394,
 'f': True,
 'np': np.array([1, 2, 3]),
 'arr': list(range(20,30, 2)),
 'l': [{'a': 1, 'b': 2, 'c': 3}, {'d': 4, 'e': 5, 'f': 6}], #dicts in list
 'd': {'a': [1, 2, 3], 'b': [4, 5, 6]},                     #lists in dict
 'lnp': [np.array([1, 2, 3]), np.array([4, 5 ,6])],         #numpy arrays in list
 'dnp': {'a': np.array([1, 2, 3]), 'b': np.array([4, 5, 6])}#numpy arrays in dict
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
    log('np:', np, ' typeof: ', typeof np);
    log('arr', arr, ' typeof: ', typeof arr, ' typeof elem: ', typeof arr[0]);
    log('l:', l, ' typeof: ', typeof l);
    log('d:', d, ' typeof: ', typeof d);
    log('lnp:', lnp, ' typeof: ', typeof lnp);
    log('dnp:', dnp, ' typeof: ', typeof dnp);

    """, vals)


    print("serialization round: ", str(i), end=" - ")
    if (check_equals(vals, out_vals)):
        print("No problem found")
    else:
        raise ValueError("Problem occured during serialization")
        #print("Problem during initial serialization")


vals = {
    'a': np.random.randn(100, 224,224, 3)
}
for i in range(5):
    start = time.time()
    out_vals = node.run("""
    console.log(a.shape, a.typedArray.length);
    """, vals)
    end = time.time()
    print("Took: ", end - start, " seconds")
    assert np.allclose( out_vals['a'], vals['a'] ), "numpy array de/serialization did not work"



parameter_space = {
    "activation": ['linear','relu','selu','sigmoid','softmax', 'tanh'],
    "optimizer": ['SGD','Adagrad','Adadelta','Adam','Adamax','RMSprop'],
    "num_layers": [1, 2, 3, 4, 5, 6],
    "num_units": [1, 2, 4, 8, 16, 32],
    "lr": [1, 0.1, 0.01, 0.001, 0.0001, 0.00001],
}
vals = {'parameter_space': parameter_space}
print(vals)


for i in range(5):
    start = time.time()
    out_vals = node.run("""
    console.log(parameter_space);
    """, vals)
    end = time.time()
    print("Took: ", end - start, " seconds")
    assert check_equals(vals, out_vals), "various parameter space de/serialization failed."

