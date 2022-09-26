from bifrost import dcp
import numpy as np

def work_function(input_slice, fs, N):
  dcp_available = False
  try:
    from js import dcp
    dcp.progress()
    dcp_available = True
  except Exception as e:
    pass

  import numpy as np
  from scipy import signal

  np.random.seed(0)

  rng = np.random.default_rng(seed=0)

  freq = input_slice

  amp = 2*np.sqrt(2)
  noise_power = 0.001 * fs / 2
  time = np.arange(N) / fs

  x = amp*np.sin(2*np.pi*freq*time)
  x += rng.normal(scale=np.sqrt(noise_power), size=time.shape)

  x[int(N//2):int(N//2)+10] *= 50.
  f, Pxx_den = signal.welch(x, fs, nperseg=4096)

  if dcp_available:
    dcp.progress()

  f_med, Pxx_den_med = signal.welch(x, fs, nperseg=4096, average='median')

  return { 'f_med': f_med, 'Pxx_den_med': Pxx_den_med, }

input_set = list(range(25))

shared_arguments = {'fs': 10e3, 'N': 1e5}

job = dcp.compute_for(input_set, work_function, shared_arguments)
job.requires(['scipy'])
job.public['name'] = "Bifrost DCP Testing : Scipy Signal"
job.pickle_work_function = False

output_set = job.exec()

compare_set = []
for compare_slice in input_set:
  compare_result = work_function(compare_slice, **shared_arguments)
  compare_set.append(compare_result)


for slice_ind in range(len(output_set)):
    cur_output_set = output_set[slice_ind]
    cur_compare_set = compare_set[slice_ind]
    for key in cur_output_set.keys():
        assert np.allclose( output_set[slice_ind][key], compare_set[slice_ind][key] ), f"Return values differ at slice {slice_ind} and key {key}"
