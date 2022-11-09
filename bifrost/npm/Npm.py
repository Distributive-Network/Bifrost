# MODULES

# python standard library
import contextlib
import io
import json
import os
import shutil
from subprocess import Popen, PIPE

# local modules
from bifrost.py_utils import is_windows, is_darwin, has_mp_shared

# PROGRAM

class Npm():
    '''
    Npm is a class that manages all calls to npm.
    '''
    def __init__(self, cwd = os.getcwd()):
        self.cwd = cwd
        self.npm_exec_path = shutil.which('npm')
        self.nodejs_major_version = int(self.nodejs_version().split(".")[0])

        self.js_needs_mmap = not os.path.exists(cwd + '/node_modules/@raygun-nickj/mmap-io')
        self.js_needs_xxhash = not os.path.exists(cwd + '/node_modules/xxhash-wasm')
        self.js_needs_shm = has_mp_shared() and not is_windows() and not is_darwin() and not os.path.exists(cwd + '/node_modules/shmmap') and self.nodejs_major_version < 16

        # TODO: find better terminology than "js needs", but favour this pattern over the previous not-and-chain approach
        if self.js_needs_mmap or self.js_needs_xxhash or self.js_needs_shm:
            npm_init_args = [
              self.npm_exec_path,
              'init',
              '--yes',
            ]
            self.run(npm_init_args)

            if self.js_needs_mmap:
                if self.nodejs_major_version < 16:
                  self.install('@raygun-nickj/mmap-io@1.2.2')
                else:
                  self.install('@raygun-nickj/mmap-io@1.3.0')
                self.js_needs_mmap = False
            if self.js_needs_xxhash:
                self.install('xxhash-wasm@0.4.2')
                self.js_needs_xxhash = False
            if self.js_needs_shm:
                self.install('git+https://github.com/chris-c-mcintyre/shmmap.js')
                self.js_needs_shm = False

    def run(self, cmd, warn=False, log=False):
        '''
        Helper function to run some command using npm.
        Useful for managing working directory of npm and where the 
        stdout pipes are pointing.

        Also helpful to block until command completes.
        '''

        # npm version notice can disrupt parsing of command outputs
        cmd.append("--no-update-notifier")
        
        process = Popen(
          cmd,
          cwd = self.cwd,
          stdout = PIPE,
          stderr = PIPE,
        )

        while True:
            output = process.stdout.readline().decode('utf-8')
            if output == '' and process.poll() is not None:
                break
            if output and log:
                print(output.strip())
            error = process.stderr.readline().decode('utf-8')
            if error and warn:
                print(error.strip())
        returnCode = process.poll()
        return returnCode

    def install(self,*args):
        self.run([self.npm_exec_path, '--quiet', 'install', *args])

    def uninstall(self, *args):
        self.run([self.npm_exec_path, '--quiet', 'uninstall', *args])

    def list_modules(self, *args):
        self.run([self.npm_exec_path, 'list', *args], warn=True, log=True)

    def package_current_version(self, package_name):
        npm_io = io.StringIO()
        with contextlib.redirect_stdout(npm_io):
            self.run([self.npm_exec_path, 'ls', package_name, '--json=true'], warn=True, log=True)
        version_json = npm_io.getvalue()
        version_dict = json.loads(version_json)
        try:
            version_string = version_dict["dependencies"][package_name]["version"]
        except KeyError:
            version_string = '0.0.0'
        return version_string.strip()

    def package_latest_version(self, package_name):
        npm_io = io.StringIO()
        with contextlib.redirect_stdout(npm_io):
            self.run([self.npm_exec_path, 'view', package_name, 'version'], warn=True, log=True)
        version_string = npm_io.getvalue()
        return version_string.strip()

    def nodejs_version(self):
        npm_io = io.StringIO()
        with contextlib.redirect_stdout(npm_io):
            self.run([self.npm_exec_path, 'version', '--json=true'], warn=True, log=True)
        version_json = npm_io.getvalue()
        version_dict = json.loads(version_json)
        version_string = version_dict["node"]
        return version_string.strip()

