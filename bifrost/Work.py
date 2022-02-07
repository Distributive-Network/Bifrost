from pathlib import Path

dcp_init_worker = Path(__file__).parent.joinpath('dcp_init_worker.py').read_text()

dcp_compute_worker = Path(__file__).parent.joinpath('dcp_compute_worker.py').read_text()

js_deploy_job = Path(__file__).parent.joinpath('dcpDeployJob.js').read_text()

js_work_function = Path(__file__).parent.joinpath('dcpWorkFunction.js').read_text()

