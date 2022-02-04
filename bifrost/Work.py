from pathlib import Path

js_deploy_job = Path('dcpJobDeploy.js').read_text()

js_work_function = Path('dcpWorkFunction.js').read_text()

dcp_init_worker = Path('dcp_init_worker.py').read_text()

dcp_compute_worker = Path('dcp_compute_worker.py').read_text()

