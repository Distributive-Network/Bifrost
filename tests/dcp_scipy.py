from bifrost import dcp

def work_function(input_slice, fs, N):

  from js import dcp
  dcp.progress()

  import numpy as np
  from scipy import signal

  np.random.seed(0)

  rng = np.random.default_rng()

  freq = input_slice

  amp = 2*np.sqrt(2)
  noise_power = 0.001 * fs / 2
  time = np.arange(N) / fs

  x = amp*np.sin(2*np.pi*freq*time)
  x += rng.normal(scale=np.sqrt(noise_power), size=time.shape)

  x[int(N//2):int(N//2)+10] *= 50.
  f, Pxx_den = signal.welch(x, fs, nperseg=4096)

  dcp.progress()

  f_med, Pxx_den_med = signal.welch(x, fs, nperseg=4096, average='median')

  return { 'f_med': f_med, 'Pxx_den_med': Pxx_den_med, }

input_set = range(25)

shared_arguments = {'fs': 10e3, 'N': 1e5}

job = dcp.compute_for(input_set, work_function, shared_arguments)
job.requires(['scipy'])
job.public['name'] = "Bifrost DCP Testing : Scipy Signal"

output_set = job.exec()

compare_set = []
for compare_slice in input_set:
  compare_result = work_function(compare_slice, **shared_arguments)
  compare_set.append(compare_result)

assert output_set == compare_set

