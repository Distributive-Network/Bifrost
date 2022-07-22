# MODULES

# python standard library
import contextlib
import io
import os

# local modules
from .Dcp import compute_do, compute_for

# CLASSES

class Url: # Temporary Implementation

    def __init__(self, url_string):
        # from urllib.parse import urlparse
        url_object = url_string # urlparse(url_string)
        self.url_object = url_object

class RangeObject: # Temporary Implementation

    # TODO: Implement 'group' support
    # TODO: Implement 'sparse' support
    def __init__(self, start, end, step, group = None):
        slices = []
        for value in range(start, end, step):
            slices.append(value)
        self.slices = slices

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, item):
        return setattr(self, item)

class MultiRangeObject: # Temporary Implementation

    # TODO: Avoid transmitting redundant inputs
    def __init__(self, ranges):
        values = []
        for value in ranges:
            if isinstance(value,(list,tuple,set)):
                values.append(value)
            else:
                values.append([value])
        slices = []
        for value in itertools.product(*values):
            slices.append(list(value))
        self.slices = slices

    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, item):
        return setattr(self, item)

class RemoteDataSet: # Temporary Implementation

    def __init__(self, url_list):
        remote_data_set = []
        for url_string in url_list:
            url_object = url_string # Url(url_string)
            remote_data_set.append(url_object)
        self.remote_data_set = remote_data_set

class RemoteDataPattern: # Temporary Implementation

    def __init__(self, url_string, url_count):
        remote_data_set = []
        for n in range(url_count):
            url_object = url_string + str(n) # Url(url_string + str(n))
            remote_data_set.append(url_object)
        self.remote_data_set = remote_data_set

# PROGRAM

def dcp_install(scheduler_url = 'https://scheduler.distributed.computer'):

    # install and initialize dcp-client

    from bifrost import node, npm

    def _parse_version(version_string):
        version_list = version_string.split('.')
        version = tuple(int(version_element) for version_element in version_list)
        return version

    def _npm_checker(package_name):

        npm_io = io.StringIO()
        with contextlib.redirect_stdout(npm_io):
            npm.list_modules(package_name)
        npm_check = npm_io.getvalue()

        if '(empty)' in npm_check:
            print('installing ' + package_name)
            npm.install(package_name)
        else:
            try:
                package_latest = npm.package_latest_version(package_name)
                package_current = npm.package_current_version(package_name)
                if _parse_version(package_current) < _parse_version(package_latest):
                    print('installing version ' + package_latest + ' of ' + package_name)
                    npm.install(package_name + '@' + package_latest)
                else:
                    print('proceeding with currently installed version of ' + package_name)
            except ValueError:
                print('installing default npm version of ' + package_name)
                npm.install(package_name)

    _npm_checker('dcp-client')

    node.run("""
    if ( !globalThis.dcpClient ) globalThis.dcpClient = require("dcp-client").init(scheduler);
    """, { 'scheduler': scheduler_url })

env_scheduler = os.environ.get('DCP_SCHEDULER_LOCATION')

if env_scheduler == None:
    dcp_scheduler = 'https://scheduler.distributed.computer'
else:
    dcp_scheduler = env_scheduler

dcp_install(dcp_scheduler)

