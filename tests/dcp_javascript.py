from bifrost import dcp, node

# Compute left Riemann sum of exp(-x^2) from 0 to b with N subintervals.
work_function = """function workFunction(b,N){

  function linspace(start, stop, num){
    var arr = [];
    var step = (stop - start) / (num - 1);
    for (var i = 0; i < num; i++) {
      arr.push(start + (step * i));
    }
    return arr;
  }

  let x = linspace(0,b,N+1);
  x.pop();

  let deltaX = b/N;
  x = x.map(a => a**2);
  I = deltaX * x.reduce((a, b) => a + b, 0);

  return I;
}"""

input_set = range(25)

shared_arguments = [ 100000 ]

job = dcp.compute_for(input_set, work_function, shared_arguments)
job.public['name'] = "Bifrost DCP Testing : Javascript Riemann Sums"
job.node_js = True

output_set = job.exec()

node_output = node.run("""

  var compareSet = [];
  for (let i = 0; i < inputSet.length; i++)
  {
    const compareSlice = inputSet[i];
    const compareResult = workFunction(compareSlice, ...sharedArguments);
    compareSet.push(compareResult);
  }

""", { "inputSet": input_set, "sharedArguments": shared_arguments })

compare_set = node_output["compareSet"]

assert output_set == compare_set

