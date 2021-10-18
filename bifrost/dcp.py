from bifrost import node, npm

import io
import contextlib

import cloudpickle
import codecs

import random

import zlib

_dcp_init_worker = """
# reset recursion limit
import sys
sys.setrecursionlimit(20000)

# suppress warnings
import warnings
warnings.filterwarnings('ignore')

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

_dcp_compute_worker = """

# input serialization
import codecs
import cloudpickle

_parameters_decoded = codecs.decode( input_parameters.encode(), 'base64' )
_parameters_unpickled = cloudpickle.loads( _parameters_decoded )

_data_decoded = codecs.decode( input_data.encode(), 'base64' )
_data_unpickled = cloudpickle.loads( _data_decoded )

import numpy as np

all_train_features = np.asarray(_parameters_unpickled['all_train_features'])

y_names = ['id','Label']
y_formats = ['int64','int64']
y_dtype = dict(names = y_names, formats = y_formats)

y_train = _parameters_unpickled['y_train']
y_train = np.asarray(list(y_train.items()), dtype = y_dtype)

_output_function = locals()[input_function]

_output_data = _output_function(
    _data_unpickled,
    all_train_features,
    y_train,)

_output_data_pickled = cloudpickle.dumps( _output_data )
output_data_encoded = codecs.encode( _output_data_pickled, 'base64' ).decode()

"""

def dcp_run(
    _job_input,
    _job_arguments,
    _job_function,
    _job_packages,
    _job_groups,
    _job_public,
    _job_multiplier,
    _job_local,
    _job_modules = [],
    _job_imports = [],
):

    global _dcp_init_worker
    global _dcp_compute_worker

    _run_parameters = {
        'dcp_data': _job_input,
        'dcp_multiplier': _job_multiplier,
        'dcp_local': _job_local,
        'dcp_groups': _job_groups,
        'dcp_public': _job_public,
        'python_init_worker': _dcp_init_worker,
        'python_compute_worker': _dcp_compute_worker,
        'python_parameters': _job_arguments,
        'python_function': _job_function,
        'python_packages': _job_packages,
        'python_modules': _job_modules,
        'python_imports': _job_imports,
    }

    _node_output = node.run("""

    (async function(){

        async function dcpPost(myData, workFunction, sharedArguments, myMultiplier, myLocal) {

            const jobStartTime = Date.now();

            let jobResults = [...Array(myData.length / myMultiplier)].map(x => []);

            let jobTimings = [];

            let distributedCount = 0;

            let compute = await require('dcp/compute');

            let inputSet = [];
            myData.forEach(x => {
              let myItem = Object.fromEntries(Object.entries(x));
              inputSet.push(myItem);
              return [];
            });

            let job = compute.for(inputSet, workFunction, sharedArguments);

            job.computeGroups = dcp_groups;

            job.public.name = dcp_public;

            //job.debug = true;

            //job.requirements.discrete = true;

            // set module requirements for python job
            job.requires('aitf-compress/pako');
            job.requires('aitf-pyodide-16/pyodide');
            job.requires('aitf-cloudpickle-16/cloudpickle');
            for (let i = 0; i < python_packages.length; i++) {
              let thisPackageName = python_packages[i];
              let thisPackagePath = 'aitf-' + thisPackageName + '-16/' + thisPackageName;
              job.requires(thisPackagePath);
            }

            let jobFunctions = {
                accepted: () => {},
                complete: () => {},
                console: () => {},
                error: () => {},
                readystatechange: () => {},
                result: () => {}
            };

            async function dcpPromise() {

                return new Promise(function(resolve, reject) {

                    jobFunctions.accepted = function onJobAccepted() {

                        console.log('Accepted: ' + job.id);
                    }

                    jobFunctions.complete = function onJobConsole(myEvent) {

                        console.log('Complete: ' + job.id);
                    }

                    jobFunctions.console = function onJobConsole(myConsole) {

                        console.log(myConsole.sliceNumber + ' : ' + myConsole.level, ' : ' + myConsole.message);
                    }

                    jobFunctions.error = function onJobError(myError) {

                        console.log(myError.sliceNumber + ' : error : ' + myError.message);
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

                            if ((emptyIndexArray.length == 0) && (myMultiplier > 1)) {

                                resolve(jobResults);
                            }

                        } else {

                            console.log('Bad Result : ' + myResult);
                        }
                    }

                    jobFunctions.readystatechange = function onJobReadyStateChange(myStateChange) {

                        console.log(myStateChange);
                    }

                    job.on('accepted', jobFunctions.accepted);
                    job.on('complete', jobFunctions.complete);
                    job.on('console', jobFunctions.console);
                    job.on('error', jobFunctions.error);
                    job.on('result', jobFunctions.result);
                    job.on('readystatechange', jobFunctions.readystatechange);

                    let execResults = [];

                    if ( myLocal > 0 ) {

                        execResults = job.localExec(myLocal);

                        resolve(execResults);

                    } else {

                        execResults = job.exec();

                        resolve(execResults);
                    }
                });
            }

            let finalResults = await dcpPromise();

            let finalOutputs;
            let finalTimings;

            if (myMultiplier > 1) {

                finalOutputs = finalResults;
                finalTimings = myTimings;

            } else {

                finalResults = await Array.from(finalResults);

                let finalOutputs = [...Array(finalResults.length)].map(x => []);
                let finalTimings = [];

                for (let i = 0; i < finalResults.length; i++) {

                    let thisResult = finalResults[i];

                    let outputIndex = thisResult.index;
                    finalOutputs[outputIndex] = thisResult.output;
                    finalTimings.push(parseInt(thisResult.elapsed, 10));
                }
            }

            job.removeEventListener('accepted', jobFunctions.accepted);
            job.removeEventListener('complete', jobFunctions.complete);
            job.removeEventListener('console', jobFunctions.console);
            job.removeEventListener('error', jobFunctions.error);
            job.removeEventListener('result', jobFunctions.result);
            job.removeEventListener('readystatechange', jobFunctions.readystatechange);

            const averageSliceTime = finalTimings.reduce((a, b) => a + b) / finalOutputs.length;
            const totalJobTime = Date.now() - jobStartTime;

            console.log('Total Elapsed Job Time: ' + (totalJobTime / 1000).toFixed(2) + ' s');
            console.log('Mean Elapsed Worker Time Per Slice: ' + averageSliceTime + ' s');
            console.log('Mean Elapsed Client Time Per Unique Slice: ' + ((totalJobTime / 1000) / finalOutputs.length).toFixed(2) + ' s');
            
            return finalOutputs;
        }
        
        let jobFunction = `async function(pythonData, pythonParameters, pythonFunction, pythonModules, pythonPackages, pythonImports, pythonInitWorker, pythonComputeWorker) {

        const providePackageFile = async function providePackageFile(packageNameArray) {

            return await new Promise((resolve, reject) => {

                try {

                    module.provide(packageNameArray, () => {

                        resolve();
                    });

                } catch(myError) {

                    reject(myError);
                };
            });
        };

        const getShardCount = async function getShardCount(packageName) {

            const entryPath = 'aitf-' + packageName + '-16/' + packageName

            await providePackageFile([entryPath]);

            const shardEntry = await require(packageName);

            const shardCount = shardEntry.PACKAGE_SHARDS;

            return shardCount;
        };

        const downloadShards = async function downloadShards(packageName) {

            progress();

            let shardCount = await getShardCount(packageName);

            for (let i = 0; i < shardCount; i++) {

                const shardName = packageName + '-shard-' + i;
                const shardPath = 'aitf-' + packageName + '-16/' + shardName;

                await providePackageFile([shardPath]);

                progress();
            }

            return true;
        };

        const decodeShards = async function decodeShards(packageName) {

            async function _loadShardCount(myPackageName) {

                const thisPackage = await require(myPackageName);

                return thisPackage.PACKAGE_SHARDS;
            }
            
            async function _loadShardData(myShardName) {

                const thisPackage = await require(myShardName);

                return thisPackage.SHARD_DATA;
            }

            async function _loadBinary(base64String) {

                let binaryString = await atob(base64String);

                const binaryLength = binaryString.length;

                let binaryArray = new Uint8Array(binaryLength);

                for(let i = 0; i < binaryLength; i++) {

                  binaryArray[i] = binaryString.charCodeAt(i);
                }

                return binaryArray;
            }

            function _arrayToString(myArray) {

                let myString = '';

                for (let i = 0; i < myArray.length; i++) {

                    myString += '%' + ('0' + myArray[i].toString(16)).slice(-2);
                }

                progress();

                myString = decodeURIComponent(myString);

                return myString;
            }

            progress();

            let decodePako = await require('pako');

            let packageInflator = new decodePako.Inflate();

            let shardCount = await _loadShardCount(packageName);

            for (let i = 0; i < shardCount; i++) {

                const shardName = packageName + '-shard-' + i;

                let shardData = await _loadShardData(shardName);

                const shardArray = await _loadBinary(shardData);

                progress();

                packageInflator.push(shardArray);
            }

            let inflatorOutput = packageInflator.result;

            progress();

            const stringChunkLength = Math.ceil(inflatorOutput.length / shardCount);

            let inflateArray = new Array(shardCount);

            for (let i = 0; i < shardCount; i++) {

                const shardStart = i * stringChunkLength;

                let stringShardData = inflatorOutput.slice(shardStart, shardStart + stringChunkLength);

                let inflateString = '';

                const stringCharLimit = 9999;

                for (let j = 0; j < Math.ceil(stringShardData.length / stringCharLimit); j++) {

                    const stringSlice = stringShardData.slice( (j * stringCharLimit), (j + 1) * stringCharLimit );

                	inflateString += String.fromCharCode.apply( null, new Uint16Array( stringSlice ) );
                }

                inflateArray[i] = inflateString;

                progress();
            }

            let finalString = await inflateArray.join('');

            progress();

            inflateArray = null;
            inflatorOutput = null;
            packageInflator = null;
            decodePako = null;

            return finalString;
        };

        const initializePyodide = async function initializePyodide(packageString) {

            eval(packageString);

            await languagePluginLoader;

            self.pyodide._module.checkABI = () => { return true };

            return true;
        };

        const initializePackage = async function initializePackage(packageString) {

            let packageFunction = await new Function(packageString);

            await packageFunction();

            return true;
        };

        const deshardPackage = async function deshardPackage(packageName, newPackage = true) {

            if (newPackage) {

                await downloadShards(packageName);
            }

            let packageString = await decodeShards(packageName);

            if (packageName == 'pyodide') {

                await initializePyodide(packageString);

            } else {

                await initializePackage(packageString);
            }

            //packageString = null;

            return true;
        };

        const setupPython = async function setupPython(packageList = []) {

            self.loadedPyodidePackages = {};

            await deshardPackage('pyodide');

            progress();

            await deshardPackage('cloudpickle');

            progress();

            for (i = 0; i < packageList.length; i++) {

                await deshardPackage(packageList[i]);

                progress();
            }
        };

        try {

            const startTime = Date.now();

            progress(0);

            let downloadPyodide = (typeof pyodide == 'undefined');
            await deshardPackage('pyodide', downloadPyodide);
            self.loadedPyodidePackages = downloadPyodide ? {} : self.loadedPyodidePackages;

            progress();

            let downloadCloudpickle = !(self.loadedPyodidePackages.hasOwnProperty('cloudpickle'));
            await deshardPackage('cloudpickle', downloadCloudpickle);
            self.loadedPyodidePackages['cloudpickle'] = true;

            progress();

            let pythonPackageCount = pythonPackages.length;

            for (i = 0; i < pythonPackageCount; i++) {

                let packageName = pythonPackages[i];

                let downloadPackage = !(self.loadedPyodidePackages.hasOwnProperty(packageName));
                await deshardPackage(packageName, downloadPackage);
                self.loadedPyodidePackages[packageName] = true;

                progress();
            }

            pyodide.globals.set('input_imports', pythonImports);
            pyodide.globals.set('input_modules', pythonModules);

            await pyodide.runPythonAsync(pythonInitWorker);

            await pyodide.runPythonAsync(pythonFunction[1]); //function.code
            
            pyodide.globals.set('input_data', pythonData.data);
            pyodide.globals.set('input_parameters', pythonParameters);
            pyodide.globals.set('input_function', pythonFunction[0]); //function.name

            await pyodide.runPythonAsync(pythonComputeWorker);

            progress();

            const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);
            
            pythonParameters = [];
            pythonFunction = [];
            pythonModules = [];
            pythonPackages = [];
            pythonImports = [];
            pythonData.data = [];
            
            progress(1);

            return {
                output: pyodide.globals.get('output_data_encoded'),
                index: pythonData.index,
                elapsed: stopTime
            };

        } catch (e) {

            throw(e);
        }
    }`;

        let jobData = dcp_data;
        let jobMultiplier = dcp_multiplier;
        let jobLocal = dcp_local;

        let jobParameters = [
            python_parameters,
            python_function,
            python_modules,
            python_packages,
            python_imports,
            python_init_worker,
            python_compute_worker
        ];

        try {

            jobOutput = await dcpPost(jobData, jobFunction, jobParameters, jobMultiplier, jobLocal);

        } catch (e) {

            await console.log('CAUGHT NODEJS ERROR : ' + e);
        }

    })();

    """, _run_parameters)

    _job_output = _node_output['jobOutput']
    
    return _job_output

def job_deploy(
        _dcp_slices,
        _dcp_function,
        _dcp_arguments = {},
        _dcp_packages = [],
        _dcp_groups = [],
        _dcp_public = { 'name': 'Bifrost Deployment'},
        _dcp_imports = [],
        _dcp_local = 0,
        _dcp_multiplier = 1):

    _job_slices = _dcp_slices
    _job_function = _dcp_function
    _job_arguments = _dcp_arguments
    _job_packages = _dcp_packages
    _job_groups = _dcp_groups
    _job_imports = _dcp_imports
    _job_public = _dcp_public
    _job_local = _dcp_local
    _job_multiplier = _dcp_multiplier

    def _input_encoder(_input_data):

        _data_encoded = codecs.encode( _input_data, 'base64' ).decode()

        return _data_encoded

    def _function_writer(_function):

        import inspect

        _function_name = _function.__name__
        _function_code = inspect.getsource(_function)

        return [_function_name, _function_code]

    def _module_writer(_module_name):

        _module_filename = _module_name + '.py'

        with open(_module_filename, 'rb') as _module:
            _module_data = _module.read()

        _module_encoded = _input_encoder( _module_data )

        return _module_encoded

    def _pickle_jar(_input_data):

        _data_pickled = cloudpickle.dumps( _input_data )
        _data_encoded = _input_encoder( _data_pickled )

        return _data_encoded

    _job_modules = {}
    for _module_name in _job_imports:
        _job_modules[_module_name] = _module_writer(_module_name)

    _job_arguments = _pickle_jar(_job_arguments)

    _job_function = _function_writer(_job_function)

    for _block_index, _block_slice in enumerate(_job_slices):

        _block_slice = _pickle_jar(_block_slice)

        _job_slices[_block_index] = {
            'index': _block_index,
            'data': _block_slice }

    _job_input = []

    for i in range(_job_multiplier):
        _job_input.extend(_job_slices)

    #random.shuffle(_job_input)

    _job_results = dcp_run(
        _job_input,
        _job_arguments,
        _job_function,
        _job_packages,
        _job_groups,
        _job_public,
        _job_modules,
        _job_imports,
        _job_multiplier,
        _job_local,
    )

    _final_results = []

    for _results_index, _results_slice in enumerate(_job_results):

        _results_slice_decoded = codecs.decode( _results_slice.encode(), 'base64' )

        _results_slice_unpickled = cloudpickle.loads( _results_slice )

        _final_results[_results_index] = _results_slice_unpickled

    print(_final_results)

    return _final_results

class Job:

    def __init__(self, input_set, work_function, work_arguments = []):

        # mandatory job arguments
        self.input_set = input_set
        self.work_function = work_function
        self.work_arguments = work_arguments

        # additional job properties
        self.compute_groups = []
        self.requires = []
        self.requirements = {}
        self.public = { 'name': 'Bifrost Deployment' }
        
        # bifrost internal job properties
        self.python_imports = []
        
    def exec(self):

        self.results = job_deploy(self.input_set, self.work_function, self.work_arguments, self.requires, self.compute_groups, self.python_imports, self.public)

    def local_exec(self, local_cores):

        self.local_cores = local_cores

        self.results = job_deploy(self.input_set, self.work_function, self.work_arguments, self.requires, self.compute_groups, self.python_imports, self.public, self.local_cores)        
    
class Dcp:

    def __init__(self, scheduler_url = 'https://scheduler.distributed.computer'):
        
        self.scheduler = scheduler_url
        
    def compute_for(self, input_set, work_function, work_arguments = []):

        job = Job(input_set, work_function, work_arguments)

        return job

dcp = Dcp()
