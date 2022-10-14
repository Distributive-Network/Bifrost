# MODULES

# python standard library
from pathlib import Path

# PROGRAM

dcp_init_worker = Path(__file__).parent.joinpath('js_work_setup.py').read_text()

dcp_compute_worker = Path(__file__).parent.joinpath('js_work_compute.py').read_text()

js_deploy_job = Path(__file__).parent.joinpath('pyJob.js').read_text()

js_work_function = Path(__file__).parent.joinpath('pyWork.js').read_text()

