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

output_data = _output_function(
    _data_unpickled,
    all_train_features,
    y_train,)

#_output_data_pickled = cloudpickle.dumps( _output_data )
#output_data_encoded = codecs.encode( _output_data_pickled, 'base64' ).decode()

"""

def dcp_run(
    _job_input,
    _job_arguments,
    _job_function,
    _job_packages,
    _job_groups,
    _job_imports,
    _job_modules,
    _job_public,
    _job_multiplier,
    _job_local,
    _job_shards
):

    global _dcp_init_worker
    global _dcp_compute_worker

    _run_parameters = {
        'dcp_input': _job_input,
        'dcp_multiplier': _job_multiplier,
        'dcp_local': _job_local,
        'dcp_groups': _job_groups,
        'dcp_public': _job_public,
        'dcp_shards': _job_shards,
        'python_init_worker': _dcp_init_worker,
        'python_compute_worker': _dcp_compute_worker,
        'python_parameters': _job_arguments,
        'python_function': _job_function,
        'python_packages': _job_packages,
        'python_modules': _job_modules,
        'python_imports': _job_imports,
    }

    node_output = node.run("""

    var jobOutput = [];
        
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

            job.public = dcp_public;

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
            
            //set module requirements for data sharding if present
            if ( (sharedArguments[0].dcpDataAddress) && (sharedArguments[0].packageNames)) {
                let dataRequiresAddress = sharedArguments[0].dcpDataAddress;
                let dataRequiresPath = dataRequiresAddress + '/' + dataRequiresAddress + '.js';
                job.requires(dataRequiresPath);
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
                                
                                console.log('Result :', myResult.result);
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

                    jobFunctions.readystatechange = function onJobReadyStateChange(myStateChange) {

                        console.log(myStateChange);
                    }

                    job.on('accepted', jobFunctions.accepted);
                    job.on('complete', jobFunctions.complete);
                    //job.on('console', jobFunctions.console);
                    job.on('error', jobFunctions.error);
                    job.on('result', jobFunctions.result);
                    job.on('readystatechange', jobFunctions.readystatechange);

                    let execResults = [];

                    if ( myLocal > 0 ) {

                        execResults = job.localExec(myLocal);

                    } else {

                        execResults = job.exec();
                    }
                });
            }

            let finalResults = await dcpPromise();

            job.removeEventListener('accepted', jobFunctions.accepted);
            job.removeEventListener('complete', jobFunctions.complete);
            //job.removeEventListener('console', jobFunctions.console);
            job.removeEventListener('error', jobFunctions.error);
            job.removeEventListener('result', jobFunctions.result);
            job.removeEventListener('readystatechange', jobFunctions.readystatechange);

            const averageSliceTime = jobTimings.reduce((a, b) => a + b) / finalResults.length;
            const totalJobTime = Date.now() - jobStartTime;

            console.log('Total Elapsed Job Time: ' + (totalJobTime / 1000).toFixed(2) + ' s');
            console.log('Mean Elapsed Worker Time Per Slice: ' + averageSliceTime + ' s');
            console.log('Mean Elapsed Client Time Per Unique Slice: ' + ((totalJobTime / 1000) / finalResults.length).toFixed(2) + ' s');
            
            return finalResults;
        }
        
        let jobFunction = `async function(pythonData, pythonParameters, pythonFunction, pythonModules, pythonPackages, pythonImports, pythonInitWorker, pythonComputeWorker) {
        
        let pythonLoaderLocal = {};

        pythonLoaderLocal.providePackageFile = async function _providePackageFile(packageNameArray) {

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

        pythonLoaderLocal.getShardCount = async function _getShardCount(packageName) {

            const entryPath = 'aitf-' + packageName + '-16/' + packageName

            await pythonLoaderLocal.providePackageFile([entryPath]);

            let shardEntry = await require(packageName);
            const shardCount = shardEntry.PACKAGE_SHARDS;
            shardEntry = null;

            return shardCount;
        };

        pythonLoaderLocal.downloadShards = async function _downloadShards(packageName) {

            progress();

            let shardCount = await pythonLoaderLocal.getShardCount(packageName);

            for (let i = 0; i < shardCount; i++) {

                const shardName = packageName + '-shard-' + i;
                const shardPath = 'aitf-' + packageName + '-16/' + shardName;

                await pythonLoaderLocal.providePackageFile([shardPath]);

                progress();
            }
        };

        pythonLoaderLocal.decodeShards = async function _decodeShards(packageName) {

            let decodeFunctions = {};
            
            decodeFunctions.loadShardCount = function _loadShardCount(myPackageName) {

                let thisPackage = require(myPackageName);
                const thisShardCount = thisPackage.PACKAGE_SHARDS;
                thisPackage = null;
                
                return thisShardCount;
            }
            
            decodeFunctions.loadShardData = function _loadShardData(myShardName) {

                let thisPackage = require(myShardName);
                const thisShardData = thisPackage.SHARD_DATA;
                thisPackage = null;

                return thisShardData;
            }

            decodeFunctions.loadBinary = function _loadBinary(base64String) {

                let binaryString = atob(base64String);
                const binaryLength = binaryString.length;

                let binaryArray = new Uint8Array(binaryLength);
                for(let i = 0; i < binaryLength; i++) {

                  binaryArray[i] = binaryString.charCodeAt(i);
                }
                binaryString = null;
                base64String = null;

                return binaryArray;
            }
            
            decodeFunctions.inflateShards = function _inflateShards(myShardCount, myPackageName) {
            
                let decodePako = require('pako');

                let packageInflator = new decodePako.Inflate();

                for (let i = 0; i < myShardCount; i++) {

                    let thisShardName = myPackageName + '-shard-' + i;

                    let thisShardData = decodeFunctions.loadShardData(thisShardName);
                    thisShardName = null;
                    
                    const thisShardArray = decodeFunctions.loadBinary(thisShardData);
                    thisShardData = null;

                    packageInflator.push(thisShardArray);
                }

                const inflatorOutput = packageInflator.result;
                packageInflator = null;
                
                return inflatorOutput;            
            }
            
            decodeFunctions.makeShardString = function _makeShardString(myStringShardData) {
                
                const stringCharLimit = 9999;

                let myInflateString = '';
                for (let j = 0; j < Math.ceil(myStringShardData.length / stringCharLimit); j++) {
                    let thisStringSlice = myStringShardData.slice( (j * stringCharLimit), (j + 1) * stringCharLimit );
                	myInflateString += String.fromCharCode.apply( null, new Uint16Array( thisStringSlice ) );
                    thisStringSlice = null;
                }
                myStringShardData = null;
                
                return myInflateString;
            }

            progress();

            let shardCount = decodeFunctions.loadShardCount(packageName);
            let inflatedShards = decodeFunctions.inflateShards(shardCount, packageName);
            
            progress();
            
            const stringChunkLength = Math.ceil(inflatedShards.length / shardCount);
            
            let packageString = '';
            for (let i = 0; i < shardCount; i++) {

                const shardStart = i * stringChunkLength;

                let stringShardData = inflatedShards.slice(shardStart, shardStart + stringChunkLength);
                let inflateString = decodeFunctions.makeShardString(stringShardData);
                stringShardData = null;

                packageString = packageString + inflateString;
                inflateString = null;
            }
            inflatedShards = null;
            
            progress();
            
            for (key in decodeFunctions) {
                if (decodeFunctions.hasOwnProperty(key)) {
                    decodeFunctions[key] = null;
                }
            }
            decodeFunctions = null;

            eval(packageString);
            if (packageName == 'pyodide') {
                await languagePluginLoader;
                self.pyodide._module.checkABI = () => { return true };
            }
            packageString = null;
        };

        pythonLoaderLocal.deshardPackage = async function _deshardPackage(packageName, newPackage = true) {
            
            if (newPackage) await pythonLoaderLocal.downloadShards(packageName);
            
            //TODO: only initialize previously loaded packages if they rely on CLAPACK or _srotg
            // currently re-initializing all python packages that are not the core pyodide package
            if (newPackage && (packageName != 'pyodide')) await pythonLoaderLocal.decodeShards(packageName);
        };

        pythonLoaderLocal.setupPython = function _setupPython(packageList = []) {

            self.loadedPyodidePackages = {};

            pythonLoaderLocal.deshardPackage('pyodide');

            progress();

            pythonLoaderLocal.deshardPackage('cloudpickle');

            progress();

            for (i = 0; i < packageList.length; i++) {

                pythonLoaderLocal.deshardPackage(packageList[i]);

                progress();
            }
        };

        try {

            const startTime = Date.now();

            progress(0);

            let downloadPyodide = (typeof pyodide == 'undefined');
            await pythonLoaderLocal.deshardPackage('pyodide', downloadPyodide);
            self.loadedPyodidePackages = downloadPyodide ? {} : self.loadedPyodidePackages;

            progress();

            let downloadCloudpickle = !(self.loadedPyodidePackages.hasOwnProperty('cloudpickle'));
            await pythonLoaderLocal.deshardPackage('cloudpickle', downloadCloudpickle);
            self.loadedPyodidePackages['cloudpickle'] = true;

            progress();

            let pythonPackageCount = pythonPackages.length;

            for (i = 0; i < pythonPackageCount; i++) {

                let packageName = pythonPackages[i];

                let downloadPackage = !(self.loadedPyodidePackages.hasOwnProperty(packageName));
                await pythonLoaderLocal.deshardPackage(packageName, downloadPackage);
                self.loadedPyodidePackages[packageName] = true;

                progress();
            }
            
            for (key in pythonLoaderLocal) {
                if (pythonLoaderLocal.hasOwnProperty(key)) {
                    pythonLoaderLocal[key] = null;
                }
            }
            pythonLoaderLocal = null;

            pyodide.globals.set('input_imports', pythonImports);
            pyodide.globals.set('input_modules', pythonModules);

            await pyodide.runPythonAsync(pythonInitWorker);

            await pyodide.runPythonAsync(pythonFunction[1]); //function.code

            if ( (pythonParameters.dcpDataAddress) && (pythonParameters.dcpDataNames) ) {
                
              if (!self.pythonArgsCache) {
              
                let loaderPath = pythonParameters.dcpDataAddress;
                let shardLoader = await require(loaderPath + '.js');

                self.pythonArgsCache = [];
                for (let i = 0; i < pythonParameters.dcpDataNames.length; i++) {

                    let shardPath = pythonParameters.dcpDataNames[i];

                    let thisShard = await shardLoader.load(shardPath);

                    self.pythonArgsCache.push(thisShard);
                }
                self.pythonArgsCache = self.pythonArgsCache.join('');
              }
              
              pythonParameters = self.pythonArgsCache;
            }
            
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
                output: pyodide.globals.get('output_data'),
                index: pythonData.index,
                elapsed: stopTime
            };

        } catch (e) {

            throw(e);
        }
    }`;

        let jobData = dcp_input;
        let jobMultiplier = dcp_multiplier;
        let jobLocal = dcp_local;

        if (dcp_shards > 0) {
          let randomAddress = require('crypto').randomBytes(25).toString('hex');
          let packageAddress = 'bifrost-argument-' + randomAddress;
          let packageNames = await dcpDataPublish(python_parameters, packageAddress, dcp_shards);
          python_parameters = { dcpDataShards: dcp_shards, dcpDataAddress: packageAddress, dcpDataNames: packageNames };
        }
        
        let jobParameters = [
            python_parameters,
            python_function,
            python_modules,
            python_packages,
            python_imports,
            python_init_worker,
            python_compute_worker
        ];
        
        jobOutput = await dcpPost(jobData, jobFunction, jobParameters, jobMultiplier, jobLocal);
    })();

    """, _run_parameters)

    job_output = node_output['jobOutput']
    
    return job_output

def job_deploy(
        _dcp_slices,
        _dcp_function,
        _dcp_arguments = {},
        _dcp_packages = [],
        _dcp_groups = [],
        _dcp_imports = [],
        _dcp_public = { 'name': 'Bifrost Deployment'},
        _dcp_local = 0,
        _dcp_multiplier = 1,
        _dcp_shards = 0):

    _job_slices = _dcp_slices
    _job_function = _dcp_function
    _job_arguments = _dcp_arguments
    _job_packages = _dcp_packages
    _job_groups = _dcp_groups
    _job_imports = _dcp_imports
    _job_public = _dcp_public
    _job_local = _dcp_local
    _job_multiplier = _dcp_multiplier
    _job_shards = _dcp_shards

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

    _job_slices_pickled = []
    for _block_index, _block_slice in enumerate(_job_slices):

        _block_slice_pickled = _pickle_jar(_block_slice)

        _job_slices_pickled.append({
            'index': _block_index,
            'data': _block_slice_pickled })
        
    _job_input = []
    for i in range(_job_multiplier):
        _job_input.extend(_job_slices_pickled)

    #random.shuffle(_job_input)

    job_results = dcp_run(
        _job_input,
        _job_arguments,
        _job_function,
        _job_packages,
        _job_groups,
        _job_imports,
        _job_modules,
        _job_public,
        _job_multiplier,
        _job_local,
        _job_shards
    )

    return job_results

    #_final_results = []

    #for _results_index, _results_slice in enumerate(job_results):

        #_results_slice_decoded = codecs.decode( _results_slice.encode(), 'base64' )

        #_results_slice_unpickled = cloudpickle.loads( _results_slice )

        #_final_results[_results_index] = _results_slice_unpickled

    #print(_final_results)

    #return _final_results

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
        self.multiplier = 1
        self.shards = 0
        
        # bifrost internal job properties
        self.python_imports = []
        
    def exec(self):

        self.local_cores = 0
        
        if (self.shards > 0):
            from .dcp_data import dcp_data_publish
            
        self.results = job_deploy(self.input_set, self.work_function, self.work_arguments, self.requires, self.compute_groups, self.python_imports, self.public, self.local_cores, self.multiplier, self.shards)
        
        return self.results

    def local_exec(self, local_cores):

        self.local_cores = local_cores
        
        if (self.shards > 0):
            from .dcp_data import dcp_data_publish

        self.results = job_deploy(self.input_set, self.work_function, self.work_arguments, self.requires, self.compute_groups, self.python_imports, self.public, self.local_cores, self.multiplier, self.shards)        

        return self.results
    
class Dcp:

    def __init__(self, scheduler_url = 'https://scheduler.distributed.computer'):
        
        self.scheduler = scheduler_url
        
    def compute_for(self, input_set, work_function, work_arguments = []):

        job = Job(input_set, work_function, work_arguments)

        return job

dcp = Dcp()
