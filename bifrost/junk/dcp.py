import sys
import os

import io
import contextlib

import zlib
import codecs

import json
import cloudpickle

dcp_init_worker = """

# reset recursion limit
import sys
sys.setrecursionlimit(20000)

# suppress warnings
import warnings
warnings.filterwarnings('ignore')

# basic libraries
import math
import time
import random

# pyodide packages
## ideally loop through provided list
import numpy as np
from scipy.stats import laplace
from sklearn import linear_model

"""

dcp_import_worker = """

# custom module loading
import sys
import codecs
import importlib.abc, importlib.util

class StringLoader(importlib.abc.SourceLoader):

    def __init__(self, data):
        self.data = data

    def get_source(self, fullname):
        return self.data
    
    def get_data(self, path):
        return codecs.decode( self.data.encode(), 'base64' )
    
    def get_filename(self, fullname):
        return './' + fullname + '.py'

def _module_runtime(module_name, module_encoded):

    module_loader = StringLoader(module_encoded)

    module_spec = importlib.util.spec_from_loader(module_name, module_loader, origin='built-in')
    module = importlib.util.module_from_spec(module_spec)
    sys.modules[module_name] = module
    module_spec.loader.exec_module(module)

for module_name in input_imports:

    module_spec = importlib.util.find_spec(module_name)

    if module_spec is None:
        _module_runtime(module_name, input_modules[module_name])

"""

dcp_decode_worker = """

# input serialization
import codecs
import cloudpickle

_parameters_unpickled = cloudpickle.loads( codecs.decode( input_parameters.encode(), 'base64' ) )
_functions_unpickled = cloudpickle.loads( codecs.decode( input_functions.encode(), 'base64' ) )
_data_unpickled = cloudpickle.loads( codecs.decode( input_data.encode(), 'base64' ) )

input_parameters = None
input_functions = None
input_data = None

"""

dcp_compute_worker = """

_function_name = _data_unpickled['function']

_output_function = _functions_unpickled[_function_name]

output_data = _output_function(
    _data_unpickled['params'],
    **_parameters_unpickled)

output_data = codecs.encode( cloudpickle.dumps( output_data ), 'base64' ).decode()

_parameters_unpickled = None
_functions_unpickled = None
_data_unpickled = None

"""

dcp_clean_worker = """

_parameters_unpickled = None
_functions_unpickled = None
_data_unpickled = None

"""

def install(job_scheduler = 'https://scheduler.distributed.computer'):

    def _npm_checker(package_name):

        npm_io = io.StringIO()
        with contextlib.redirect_stdout(npm_io):
            npm.list_modules(package_name)
        npm_check = npm_io.getvalue()

        if '(empty)' in npm_check:
            npm.install(package_name, '--silent')

    for name in ['dcp-client']:
        _npm_checker(name)

    install_parameters = {
        'dcp_scheduler': job_scheduler
    }

    install_output = node.run("""

    const process = require('process');
    const fs = require('fs');

    process.argv.push('--dcp-scheduler', dcp_scheduler)

    require('dcp-client').initSync(process.argv);

    const compute = require('dcp/compute');
    const wallet  = require('dcp/wallet');
    const dcpCli = require('dcp/cli');

    var accountKeystore;

    async function loadKeystore() {

        identityKeystore = await dcpCli.getIdentityKeystore();
        wallet.addId(identityKeystore);
        accountKeystore = await dcpCli.getAccountKeystore();
    }

    var jobOutput = [];

    """, install_parameters)

    return True

def wallet():

    wallet_output = node.run("""

    loadKeystore();

    """)

    return True

def python_job(
        job_data,
        job_function,
        job_multiplier,
        job_local,
        job_input,
        job_packages,
        job_modules):

    run_parameters = {
        'dcp_data': job_data,
        'dcp_function': job_function,
        'dcp_multiplier': job_multiplier,
        'dcp_local': job_local,
        'dcp_parameters': job_input,
        'dcp_packages': job_packages,
        'dcp_modules': job_modules
    }

    python_output = node.run("""

        jobRequires.push(
            'aitf-compress/pako',
            'aitf-shard-loader/shard-loader',
            'aitf-pyodide/pyodide',
            'aitf-cloudpickle/cloudpickle'
        );

        for (let i = 0; i < dcp_packages.length; i++) {

            let packageName = dcp_packages[i];
            jobRequires.push('aitf-' + packageName + '/' + packageName);
        }

        jobOutput = 0;

    """, run_parameters)

    job_output = node_output['jobOutput']
    
        node_output = node.run("""

    (async function(){

        async function dcpPost(myData, myFunction, myParameters, myMultiplier, myLocal, myRequires) {

            let pyodideJob = true;
            
            const jobStartTime = Date.now();

            let jobResults = [...Array(myData.length / myMultiplier)].map(x => []);

            let jobTimings = [];

            let distributedCount = 0;

            let job = compute.for(myData, myFunction, myParameters);

            job.public.name = 'AITF : Deployed From Python';
            
            job.requires(myRequires);

            let jobFunctions = {
                accepted: () => {},
                console: () => {},
                error: () => {},
                status: () => {},
                result: () => {}
            };

            async function dcpPromise() {

                return new Promise(async function(resolve, reject) {

                    jobFunctions.accepted = function onJobAccepted() {

                        console.log('Accepted: ' + job.id);
                    }

                    jobFunctions.console = function onJobConsole(myConsole) {

                        console.log(myConsole);
                    }

                    jobFunctions.error = function onJobError(myError) {

                        console.log(myError);
                    }

                    jobFunctions.status = function onJobStatus(myStatus) {

                        console.log(myStatus);

                        if (myStatus.distributed > distributedCount) {

                            distributedCount = myStatus.distributed;

                            let percentDistributed = ((distributedCount / myData.length ) * 100).toFixed(2);
                            console.log('Distributed : ' + percentDistributed + '%');
                        }
                    }

                    jobFunctions.result = function onJobResult(myResult) {

                        if (myResult.result.hasOwnProperty('output')) {

                            if (jobResults[myResult.result.index].length == 0) {

                                jobResults[myResult.result.index] = myResult.result.output;

                                jobTimings.push(parseInt(myResult.result.elapsed, 10));

                                let percentComputed = ((jobTimings.length / jobResults.length) * 100).toFixed(2);
                                console.log('Computed: ' + percentComputed + '%');
                            }

                            let emptyIndexArray = jobResults.filter(thisResult => thisResult.length == 0);

                            console.log('Unique Slices Remaining : ' + emptyIndexArray.length);

                            if (emptyIndexArray.length == 0) {

                                resolve(jobResults);
                            }

                        } else {

                            console.log('Bad Result : ' + myResult);
                        }
                    }

                    job.on('accepted', jobFunctions.accepted);
                    job.on('console', jobFunctions.console);
                    job.on('error', jobFunctions.error);
                    job.on('status', jobFunctions.status);
                    job.on('result', jobFunctions.result);

                    if ( myLocal > 0 ) {

                        await job.localExec(myLocal, compute.marketValue, accountKeystore);

                    } else {

                        await job.exec(compute.marketValue, accountKeystore);
                    }
                });
            }

            let finalResults = await dcpPromise();

            job.removeEventListener('accepted', jobFunctions.accepted);
            job.removeEventListener('console', jobFunctions.console);
            job.removeEventListener('error', jobFunctions.error);
            job.removeEventListener('status', jobFunctions.status);
            job.removeEventListener('result', jobFunctions.result);

            job = null;

            const averageSliceTime = jobTimings.reduce((a, b) => a + b) / jobResults.length;
            const totalJobTime = Date.now() - jobStartTime;

            console.log('Total Elapsed Job Time: ' + (totalJobTime / 1000).toFixed(2) + ' s');
            console.log('Mean Elapsed Worker Time Per Slice: ' + averageSliceTime + ' s');
            console.log('Mean Elapsed Client Time Per Unique Slice: ' + ((totalJobTime / 1000) / jobResults.length).toFixed(2) + ' s');

            return finalResults;
        }
/*
        let jobFunction = `async function(sliceData, sliceParameters) {

          try {

            const startTime = Date.now();

            progress(0);

            await console.log('Starting worker function for slice ' + sliceData.index + ' at ' + startTime + '...');

            let sliceFunction = ${dcp_function};

            let sliceOutput = await sliceFunction(sliceData.data, sliceParameters);

            progress(1);

            const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);

            return {
                output: sliceOutput,
                index: sliceData.index,
                elapsed: stopTime
            };

          } catch (e) {

            await console.log(e);

            throw e;
          }

        }`;
*/
        let jobFunction = `async function(pythonData, pythonInit, pythonImport, pythonDecode, pythonCompute, pythonClean, pythonParameters, pythonFunctions, pythonModules, pythonPackages, pythonImports) {

          const startTime = Date.now();

          try {

            progress(0);

            await console.log('Starting worker function at ' + startTime + '...');

            if (typeof sandboxTracker !== 'undefined') {

                sandboxTracker = sandboxTracker + 1;

                await console.log('Prior Sandbox Iterations : ' + sandboxTracker);

            } else {

                sandboxTracker = 0;
            }
            
            let pythonLoader = await require('shard-loader');
            await console.log('Loaded pythonLoader');

            if (typeof pyodide !== 'undefined') {

                await console.log('Preloaded pyodide');

            } else {

                self.loadedPyodidePackages = {};

                async function initializePyodideFunction(pyodideFunction) {

                    pyodideFunction = await new Function(pyodideFunction);

                    pyodideFunction();

                    try {

                        await Module.isDoneLoading(progress);

                    } catch(err) {

                        try {

                            await pyodide.isDoneLoading(progress);

                        } catch(er) {

                            while (typeof pyodide.isDoneLoading === 'undefined'){

                                await new Promise((resolve, reject)=> setTimeout(resolve, Math.ceil(Math.random() * 1000)));

                                progress();
                            }

                            await pyodide.isDoneLoading(progress);
                        }
                    };

                    return true;
                }

                await console.log('Loading pyodide...');

                const shardCount = await require('pyodide').PACKAGE_SHARDS;

                await pythonLoader.download('pyodide', shardCount);
                let decodedString = await pythonLoader.decode('pyodide', shardCount);

                await console.log('Initializing pyodide...');

                let decodedFlag = await initializePyodideFunction(decodedString);

                decodedString = null;

                await console.log('Initialized pyodide!');
            }

            if (self.loadedPyodidePackages.hasOwnProperty('cloudpickle')) {

                await console.log('Preloaded cloudpickle');

            } else {

                await require('cloudpickle').deshard();

                await console.log('Loaded cloudpickle');

                self.loadedPyodidePackages['cloudpickle'] = true;
            }

            let pythonPackageCount = pythonPackages.length;

            for (i = 0; i < pythonPackageCount; i++) {

                let packageName = pythonPackages[i];

                if (self.loadedPyodidePackages.hasOwnProperty(packageName)) {

                    await console.log('Preloaded ' + packageName);

                } else {

                    await require(packageName).deshard();

                    await console.log('Loaded ' + packageName);

                    self.loadedPyodidePackages[packageName] = true;
                }

                progress();
            }

            await console.log(Object.getOwnPropertyNames(self.loadedPyodidePackages));

            await pyodide.loadPackage('cloudpickle');

            for (i = 0; i < pythonPackageCount; i++) {
            
                let packageName = pythonPackages[i];
                
                await pyodide.loadPackage(packageName);

                progress();
            }
            
            pyodide.globals.input_imports = pythonImports;//pyodide.globals.set('input_imports', pythonImports);
            pyodide.globals.input_modules = pythonModules;//pyodide.globals.set('input_modules', pythonModules);
            pyodide.globals.input_data = pythonData.data;//pyodide.globals.set('input_data', pythonData.data);
            pyodide.globals.input_parameters = pythonParameters;//pyodide.globals.set('input_parameters', pythonParameters);
            pyodide.globals.input_functions = pythonFunctions;//pyodide.globals.set('input_functions', pythonFunctions);

            progress();

            await pyodide.runPython(pythonInit);
            //await pyodide.runPythonAsync(pythonInit);

            progress();

            //await pyodide.runPythonAsync(pythonImport);
            await pyodide.runPython(pythonImport);

            progress();

            //await pyodide.runPythonAsync(pythonDecode);
            await pyodide.runPython(pythonDecode);

            progress();

            //await pyodide.runPythonAsync(pythonCompute);
            await pyodide.runPython(pythonCompute);

            progress();

            //await pyodide.runPythonAsync(pythonClean);
            await pyodide.runPython(pythonClean);

            const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);

            pythonInit = [];
            pythonImport = [];
            pythonDecode = [];
            pythonCompute = [];
            pythonClean = [];
            pythonParameters = [];
            pythonFunctions = [];
            pythonModules = [];
            pythonPackages = [];
            pythonImports = [];
            pythonData.data = [];

            progress(1);

            return {
                output: pyodide.globals.output_data,// pyodide.globals.get('output_data'),
                index: pythonData.index,
                elapsed: stopTime
            };

          } catch (e) {

            const errorTime = ((Date.now() - startTime) / 1000).toFixed(2);
            
            await console.log(e);
            return {
                output: 0,//pyodide.globals.get('output_data'),
                index: pythonData.index,//pythonData.index,
                elapsed: errorTime
            };
          }
        }`;
        
        let jobData = dcp_data;
        let jobMultiplier = dcp_multiplier;
        let jobLocal = dcp_local;

        let jobParameters = dcp_parameters;

        let jobRequires = [];

        if (pyodideJob) jobRequires.push(
            'aitf-compress/pako',
            'aitf-shard-loader/shard-loader',
            'aitf-pyodide/pyodide',
            'aitf-cloudpickle/cloudpickle'
        );

        for (let i = 0; i < dcp_packages.length; i++) {

            jobRequires.push(dcp_packages[i]);
        }

        jobOutput = await dcpPost(jobData, jobFunction, jobParameters, jobMultiplier, jobLocal, jobRequires);

    })();

    """, run_parameters)

    job_output = node_output['jobOutput']
    
    return job_output

def nodejs_job(
        job_data,
        job_function,
        job_multiplier,
        job_local,
        job_input,
        job_packages):

    run_parameters = {
        'dcp_data': job_data,
        'dcp_function': job_function,
        'dcp_multiplier': job_multiplier,
        'dcp_local': job_local,
        'dcp_parameters': job_input,
        'dcp_packages': job_packages
    }

    node_output = node.run("""

    (async function(){

        async function dcpPost(myData, myFunction, myParameters, myMultiplier, myLocal, myRequires) {

            const jobStartTime = Date.now();

            let jobResults = [...Array(myData.length / myMultiplier)].map(x => []);

            let jobTimings = [];

            let distributedCount = 0;

            let job = compute.for(myData, myFunction, myParameters);

            job.public.name = 'AITF : Deployed From Python';
            
            job.requires(myRequires);

            let jobFunctions = {
                accepted: () => {},
                console: () => {},
                error: () => {},
                status: () => {},
                result: () => {}
            };

            async function dcpPromise() {

                return new Promise(async function(resolve, reject) {

                    jobFunctions.accepted = function onJobAccepted() {

                        console.log('Accepted: ' + job.id);
                    }

                    jobFunctions.console = function onJobConsole(myConsole) {

                        console.log(myConsole);
                    }

                    jobFunctions.error = function onJobError(myError) {

                        console.log(myError);
                    }

                    jobFunctions.status = function onJobStatus(myStatus) {

                        console.log(myStatus);

                        if (myStatus.distributed > distributedCount) {

                            distributedCount = myStatus.distributed;

                            let percentDistributed = ((distributedCount / myData.length ) * 100).toFixed(2);
                            console.log('Distributed : ' + percentDistributed + '%');
                        }
                    }

                    jobFunctions.result = function onJobResult(myResult) {

                        if (myResult.result.hasOwnProperty('output')) {

                            if (jobResults[myResult.result.index].length == 0) {

                                jobResults[myResult.result.index] = myResult.result.output;

                                jobTimings.push(parseInt(myResult.result.elapsed, 10));

                                let percentComputed = ((jobTimings.length / jobResults.length) * 100).toFixed(2);
                                console.log('Computed: ' + percentComputed + '%');
                            }

                            let emptyIndexArray = jobResults.filter(thisResult => thisResult.length == 0);

                            console.log('Unique Slices Remaining : ' + emptyIndexArray.length);

                            if (emptyIndexArray.length == 0) {

                                resolve(jobResults);
                            }

                        } else {

                            console.log('Bad Result : ' + myResult);
                        }
                    }

                    job.on('accepted', jobFunctions.accepted);
                    job.on('console', jobFunctions.console);
                    job.on('error', jobFunctions.error);
                    job.on('status', jobFunctions.status);
                    job.on('result', jobFunctions.result);

                    if ( myLocal > 0 ) {

                        await job.localExec(myLocal, compute.marketValue, accountKeystore);

                    } else {

                        await job.exec(compute.marketValue, accountKeystore);
                    }
                });
            }

            let finalResults = await dcpPromise();

            job.removeEventListener('accepted', jobFunctions.accepted);
            job.removeEventListener('console', jobFunctions.console);
            job.removeEventListener('error', jobFunctions.error);
            job.removeEventListener('status', jobFunctions.status);
            job.removeEventListener('result', jobFunctions.result);

            job = null;

            const averageSliceTime = jobTimings.reduce((a, b) => a + b) / jobResults.length;
            const totalJobTime = Date.now() - jobStartTime;

            console.log('Total Elapsed Job Time: ' + (totalJobTime / 1000).toFixed(2) + ' s');
            console.log('Mean Elapsed Worker Time Per Slice: ' + averageSliceTime + ' s');
            console.log('Mean Elapsed Client Time Per Unique Slice: ' + ((totalJobTime / 1000) / jobResults.length).toFixed(2) + ' s');

            return finalResults;
        }

        let jobFunction = `async function(sliceData, sliceParameters) {

          try {

            const startTime = Date.now();

            progress(0);

            await console.log('Starting worker function for slice ' + sliceData.index + ' at ' + startTime + '...');

            let sliceFunction = ${dcp_function};

            let sliceOutput = await sliceFunction(sliceData.data, sliceParameters);

            progress(1);

            const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);

            return {
                output: sliceOutput,
                index: sliceData.index,
                elapsed: stopTime
            };

          } catch (e) {

            await console.log(e);

            throw e;
          }

        }`;
        
        let jobData = dcp_data;
        let jobMultiplier = dcp_multiplier;
        let jobLocal = dcp_local;

        let jobParameters = dcp_parameters;

        let jobRequires = [];

        for (let i = 0; i < dcp_packages.length; i++) {

            jobRequires.push(dcp_packages[i]);
        }

        jobOutput = await dcpPost(jobData, jobFunction, jobParameters, jobMultiplier, jobLocal, jobRequires);

    })();

    """, run_parameters)

    job_output = node_output['jobOutput']
    
    return job_output

def deploy(
        job_slices,
        job_function,
        job_arguments = [],
        job_packages = [],
        job_multiplier = 1,
        job_local = 0,
        job_randomize = False,
        job_convert = False,
        job_compress = False,
        job_encode = False,
        job_pyodide = False,
        python_imports = [],
        python_functions = {}):

    _job_slices = job_slices
    _job_function = job_function
    _job_arguments = job_arguments
    _job_packages = job_packages
    _job_multiplier = job_multiplier
    _job_local = job_local
    _job_randomize = job_randomize
    _job_convert = job_convert
    _job_compress = job_compress
    _job_encode = job_encode
    _job_pyodide = job_pyodide
    _python_imports = python_imports
    _python_functions = python_functions
    
    def _module_writer(module_name):

        module_filename = module_name + '.py'

        with open(module_filename, 'rb') as module:
            module_data = module.read()

        module_encoded = codecs.encode( module_data, 'base64' ).decode()

        return module_encoded

    def _pickle_jar(input_data):
        
        if _job_convert:
            if _job_pyodide:
                data_pickled = cloudpickle.dumps( input_data )
            else:
                data_pickled = json.dumps( input_data )
        else:
            data_pickled = input_data
        
        if _job_compress:
            data_pickled = zlib.compress( data_pickled )
            
        if _job_encode:
            data_pickled = codecs.encode( data_pickled, 'base64' ).decode() # BREAKAGE IN NODEJS?

        return data_pickled
        
    # def _json_jar():
    # ???
    
    # def _crypt_jar():
    # ???
    
    _python_files = {}
    for module_name in _python_imports:
        _python_files[module_name] = _module_writer(module_name)

    _job_arguments = _pickle_jar(_job_arguments)
    _python_functions = _pickle_jar(_python_functions)

    for block_index, block_slice in enumerate(_job_slices):

        block_slice = _pickle_jar(block_slice)

        _job_slices[block_index] = {
            'index': block_index,
            'data': block_slice }

    _job_input = []

    for i in range(_job_multiplier):
        _job_input.extend(_job_slices)

    if _job_randomize:
        random.shuffle(_job_input)

"""
    dcp_init_worker, dcp_import_worker, dcp_decode_worker, dcp_compute_worker, dcp_clean_worker
"""

    _python_runtime = dcp_init_worker + dcp_import_worker + dcp_decode_worker + dcp_compute_worker + dcp_clean_worker
    
    if _job_pyodide:
        job_results = python_job(
                _job_input,
                _job_function,
                _job_multiplier,
                _job_local,
                _job_arguments,
                _job_packages,
                _python_files,
                _python_functions,
                _python_runtime)
    else:
        job_results = nodejs_job(
                _job_input,
                _job_function,
                _job_multiplier,
                _job_local,
                _job_arguments,
                _job_packages)

    for results_index, results_slice in enumerate(job_results):

        if _job_encode:
            results_slice = codecs.decode( results_slice.encode(), 'base64' ) # BREAKAGE IN NODEJS?

        if _job_compress:
            results_slice = zlib.decompress( results_slice )
            
        if _job_convert:
            if _job_pyodide:
                results_slice = cloudpickle.loads( results_slice )
            else:
                results_slice = json.loads( results_slice )

        job_results[results_index] = results_slice

    return job_results

