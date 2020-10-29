import time
from bifrost import node, npm

def check_equals(in_vals, out_vals):
    flag = False
    for key in in_vals.keys(): 
        if key in out_vals:
            if (out_vals[key] != in_vals[key]):
                print(key, " is not equivalent! problem during serialization")
                flag = True
            else:
                continue
    return flag


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
    if (not check_equals(vals, out_vals)):
        print("No problem found")
    else:
        print("Problem during initial serialization")
