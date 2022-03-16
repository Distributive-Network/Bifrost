from bifrost import node, npm



vals = {
 'n': 35,
 'm': 128,
 'c': 7.6981928384058191820394,
 'f': True,
 'd': None,
 'arr': list(range(20,30, 2))
}



out_vals = node.run("""

log = (vals)=>{console.log("from JS\n", ...vals)};

log("Beginning with the simple numbers!: ",
    "typeof n", typeof n,"\n",
    "typeof m", typeof m,"\n",
    "typeof c", typeof c,"\n");
""", vals)

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

print("First serialization round.")
if (check_equals(vals, out_vals)):
    print("No problem found")
else:
    print("Problem during initial serialization")




new_out_vals = node.run("""

log = (vals)=>{console.log("from JS\n", ...vals)};

log("Beginning with the simple numbers!: ",
    "typeof n", typeof n,"\n",
    "typeof m", typeof m,"\n",
    "typeof c", typeof c,"\n");
""", out_vals)


print("Second serialization round.")
if (check_equals(out_vals, new_out_vals)):
    print("No problem found")
else:
    print("Problem during second serialization")


