from bifrost import dcp

def work_function(input_slice):
  if (input_slice[0] % 2) == 0:
    return input_slice[1].lower()
  else:
    return input_slice[1].upper()

input_string = "Hello, my name is Bob, and this is my Bifrost test string!"
input_set = []
for idx, val in enumerate(list(input_string)):
  input_set.append(tuple([idx, val]))

job = dcp.compute_for(input_set, work_function)
job.public['name'] = "Bifrost DCP Testing : Spongecase"
job.pickle_work_function = False

output_set = job.exec()

compare_set = []
for compare_slice in input_set:
  compare_result = work_function(compare_slice)
  compare_set.append(compare_result)

assert output_set == compare_set

