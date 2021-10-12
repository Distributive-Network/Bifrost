# colab_interface.py

## NOTE: THIS CELL REPRESENTS A CUSTOM SCRIPT OR MODULE, AND WILL NOT BE EXPOSED IN THE FINAL DELIVERABLE
## NOTE: THIS PARTICULAR SCRIPT IS A DEV VERSION OF BIFROST DEPLOYMENT CODE, AND SO WILL CALLED VIA BIFROST

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

_functions_decoded = codecs.decode( input_functions.encode(), 'base64' )
_functions_unpickled = cloudpickle.loads( _functions_decoded )

_data_decoded = codecs.decode( input_data.encode(), 'base64' )
_data_unpickled = cloudpickle.loads( _data_decoded )

from numpy import asarray

def naive_bayes_gs(params, all_train_features, y_train):
    
    from sklearn.naive_bayes import MultinomialNB
    mnb_clf = MultinomialNB(**params)

    mnb_model = mnb_clf.fit(all_train_features, y_train)
    mnb_score = mnb_model.score(all_train_features, y_train)
    
    print('~~> SCORE : ', mnb_score, ' ( PARAMS : ', params, ' )')
    
    return mnb_score

def support_vector_gs(params, all_train_features, y_train):

    from sklearn.svm import LinearSVC
    svc_clf = LinearSVC(**params)

    svc_model = svc_clf.fit(all_train_features, y_train)
    svc_score = svc_model.score(all_train_features, y_train)
    
    print('~~> SCORE : ', svc_score, ' ( PARAMS : ', params, ' )')
    
    return svc_score

def logistic_regression_gs(params, all_train_features, y_train):

    from sklearn.linear_model import LogisticRegression
    lrc_clf = LogisticRegression(**params)

    lrc_model = lrc_clf.fit(all_train_features, y_train)
    lrc_score = lrc_model.score(all_train_features, y_train)
    
    print('~~> SCORE : ', lrc_score, ' ( PARAMS : ', params, ' )')

    return lrc_score

def random_forest_gs(params, all_train_features, y_train):
    
    from sklearn.ensemble import RandomForestClassifier
    rfc_clf = RandomForestClassifier(**params)
    
    rfc_model = rfc_clf.fit(all_train_features, y_train)
    rfc_score= rfc_model.score(all_train_features, y_train)
    
    print('~~> SCORE : ', rfc_score, ' ( PARAMS : ', params, ' )')

    return rfc_score

_functions_local = {
  'naive_bayes_gs': naive_bayes_gs,
  'support_vector_gs': support_vector_gs,
  'logistic_regression_gs': logistic_regression_gs,
  'random_forest_gs': random_forest_gs,
}
_function_name = _data_unpickled['function']

#_output_function = _functions_unpickled[_function_name]
_output_function = _functions_local[_function_name]

all_train_features = _parameters_unpickled['all_train_features']
y_train = _parameters_unpickled['y_train']

all_train_features = asarray(_parameters_unpickled['all_train_features'])
#all_train_features = np.asarray(_parameters_unpickled['all_train_features'])

y_train = _parameters_unpickled['y_train']
y_names = ['id','Label']
y_formats = ['int64','int64']
y_dtype = dict(names = y_names, formats = y_formats)
#y_train = np.asarray(list(y_train.items()), dtype = y_dtype)
y_train = asarray(list(y_train.items()), dtype = y_dtype)

##output_data = _output_function(
##    _data_unpickled['params'],
##    **_parameters_unpickled)

##output_data = _functions_unpickled['display_string'](
##        _data_unpickled['input_index'],
##        **_parameters_unpickled)

_output_data = _output_function(
    _data_unpickled['params'],
    all_train_features,
    y_train,)

_output_data_pickled = cloudpickle.dumps( _output_data )
output_data_encoded = codecs.encode( _output_data_pickled, 'base64' ).decode()

#output_data = codecs.encode( cloudpickle.dumps( output_data ), 'base64' ).decode()
"""

def dcp_run(_job_data, _job_multiplier, _job_local, _job_input, _job_functions, _job_modules, _job_packages, _job_imports):

    #print('%%% dcp_run :: function started %%%')

    global _dcp_init_worker
    global _dcp_compute_worker

    _run_parameters = {
        'dcp_data': _job_data,
        'dcp_multiplier': _job_multiplier,
        'dcp_local': _job_local,
        'python_init_worker': _dcp_init_worker,
        'python_compute_worker': _dcp_compute_worker,
        'python_parameters': _job_input,
        'python_functions': _job_functions,
        'python_modules': _job_modules,
        'python_packages': _job_packages,
        'python_imports': _job_imports
    }

    #print(type(_run_parameters['dcp_data']))
    #print(type(_run_parameters['dcp_multiplier']))
    #print(type(_run_parameters['dcp_local']))
    #print(type(_run_parameters['python_parameters']))
    #print(type(_run_parameters['python_functions']))
    #print(type(_run_parameters['python_modules']))
    #print(type(_run_parameters['python_packages']))
    #print(type(_run_parameters['python_imports']))

    #print('%%% dcp_run :: _run_parameters defined %%%')

    _node_output = node.run("""

    (async function(){

        async function dcpPost(myData, workFunction, myParameters, myMultiplier, myLocal) {

            const jobStartTime = Date.now();

            let jobResults = [...Array(myData.length / myMultiplier)].map(x => []);

            let jobTimings = [];

            let distributedCount = 0;

            let compute = await require('dcp/compute');

            let inputSet = myData;//[];
            /*
            myData.forEach(x => {
              let myItem = Object.fromEntries(Object.entries(x));
              inputSet.push(myItem);
              return [];
            });
            */
            let sharedArguments = myParameters;//[];
            /*
            myParameters.forEach(x => {
                let myItem = Object.fromEntries(Object.entries(x));
                sharedArguments.push(myItem);
                return [];
            });
            */

            await console.log('< WORK FUNCTION -');
            await console.log(workFunction);
            await console.log('- WORK FUNCTION >');

            //await console.log('< SHARED ARGUMENTS -');
            //await console.log(sharedArguments);
            //await console.log('- SHARED ARGUMENTS >');

            await console.log('< INPUT SET -');
            await console.log(inputSet);
            await console.log('- INPUT SET >');

            let job = compute.for(inputSet, workFunction, sharedArguments);

            await console.log('!!! JS dcpPost :: compute.for handler initialized');

            //job.computeGroups=[{joinKey: 'lakehead-edge', joinSecret: 'WdhoyVrtdXwFZuynzbTNP3WzNvspV61GMbHP7GINZv4KMQzGeW'}];
            //job.computeGroups=[{joinKey: 'soscip-edge', joinSecret: 'da9c1c90-d424-11eb-9147-47da37455484'}];
            //job.computeGroups=[{joinKey: 'anu-edge', joinSecret: 'idMMZpFPu6'}];
            //job.computeGroups=[{joinKey: 'chris', joinSecret: 'aristrocrats'}];
            job.computeGroups=[{joinKey: 'aitf', joinSecret: '9YDEXdihud'}];

            job.public.name = 'ANU : Dev Colab';

            //job.debug = true;

            job.requirements.discrete = true;

            job.requires([
                'aitf-compress/pako',
                //'aitf-shard-loader-16/shard-loader',
                //'aitf-python-loader-16/shard-loader',
                //'aitf-cache-loader-16/shard-loader',
                'aitf-pyodide-16/pyodide',
                'aitf-cloudpickle-16/cloudpickle',
                'aitf-numpy-16/numpy',
                'aitf-scipy-16/scipy',
                'aitf-joblib-16/joblib',
                'aitf-scikit-learn-16/scikit-learn',
            ]);

            let jobFunctions = {
                accepted: () => {},
                complete: () => {},
                console: () => {},
                error: () => {},
                readystatechange: () => {},
                result: () => {}
            };

            async function dcpPromise() {

                await console.log('!!! JS dcpPromise :: function started');

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

            await console.log('!!! JS dcpPost :: promise defined');

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

            await console.log('!!! JS dcpPost :: promise returned');

            await console.log(finalOutputs);

            job.removeEventListener('accepted', jobFunctions.accepted);
            job.removeEventListener('complete', jobFunctions.complete);
            job.removeEventListener('console', jobFunctions.console);
            job.removeEventListener('error', jobFunctions.error);
            job.removeEventListener('result', jobFunctions.result);
            job.removeEventListener('readystatechange', jobFunctions.readystatechange);

            //job = null;

            const averageSliceTime = finalTimings.reduce((a, b) => a + b) / finalOutputs.length;
            const totalJobTime = Date.now() - jobStartTime;

            console.log('Total Elapsed Job Time: ' + (totalJobTime / 1000).toFixed(2) + ' s');
            console.log('Mean Elapsed Worker Time Per Slice: ' + averageSliceTime + ' s');
            console.log('Mean Elapsed Client Time Per Unique Slice: ' + ((totalJobTime / 1000) / finalOutputs.length).toFixed(2) + ' s');
            
            return finalOutputs;
        }


        let jobFunction_test = `async function(pythonData, pythonParameters, pythonFunctions, pythonModules, pythonPackages, pythonImports) {

            progress();

            await console.log(pythonData);

            await console.log(pythonPackages);

            return {
                output: pythonData,
                index: pythonPackages.length,
                elapsed: 0
            };
        }`;
        
        let jobFunction = `async function(pythonData, pythonParameters, pythonFunctions, pythonModules, pythonPackages, pythonImports, pythonInitWorker, pythonComputeWorker) {

        const getPackageVersion = async function getPackageVersion() {

            const myPackageVersion = '0.6.1'; // this is for testing; don't @ me.

            return myPackageVersion;
        };

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

            //await pyodide.runPythonAsync('output_data_encoded = "HELL0"');

            pyodide.globals.set('input_imports', pythonImports);
            pyodide.globals.set('input_modules', pythonModules);

            await pyodide.runPythonAsync(pythonInitWorker);

            pyodide.globals.set('input_data', pythonData.data);
            pyodide.globals.set('input_parameters', pythonParameters);
            pyodide.globals.set('input_functions', pythonFunctions);

            await pyodide.runPythonAsync(pythonComputeWorker);

            progress();

            const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);
            
            pythonParameters = [];
            pythonFunctions = [];
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

            console.log(e);

            throw(e);
        }
    }`;

        let jobData = dcp_data;
        let jobMultiplier = dcp_multiplier;
        let jobLocal = dcp_local;

        let jobParameters = [
            python_parameters,
            python_functions,
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

            jobOutput = 'lol'
        }

    })();

    """, _run_parameters)

    _job_output = _node_output['jobOutput']
    
    return _job_output

def job_deploy(
        _dcp_slices,
        _dcp_functions = {},
        _dcp_arguments = {},
        _dcp_imports = [],
        _dcp_packages = [],
        _dcp_multiplier = 1,
        _dcp_local = 0):

    _job_slices = _dcp_slices
    _job_functions = _dcp_functions
    _job_arguments = _dcp_arguments
    _job_imports = _dcp_imports
    _job_packages = _dcp_packages
    _job_multiplier = _dcp_multiplier
    _job_local = _dcp_local

    def _input_encoder(_input_data):

        _data_encoded = codecs.encode( _input_data, 'base64' ).decode()

        return _data_encoded

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

    _job_functions = _pickle_jar(_job_functions)

    for _block_index, _block_slice in enumerate(_job_slices):

        _block_slice = _pickle_jar(_block_slice)

        _job_slices[_block_index] = {
            'index': _block_index,
            'data': _block_slice }

    _job_input = []

    for i in range(_job_multiplier):
        _job_input.extend(_job_slices)

    #random.shuffle(_job_input)

    _job_results = dcp_run(_job_input, _job_multiplier, _job_local, _job_arguments, _job_functions, _job_modules, _job_packages, _job_imports)

    _final_results = []

    if (_job_results == 'lol'):

        print('double lol')

        _final_results = _job_results

    else:

        for _results_index, _results_slice in enumerate(_job_results):

            print('&&& job_deploy :: slice #', str(_results_index), ' data_baseline ::', _results_slice)

            _results_slice_decoded = codecs.decode( _results_slice.encode(), 'base64' )

            print('&&& job_deploy :: slice #', str(_results_index), ' data_decoded ::', _results_slice_decoded)

            _results_slice_unpickled = cloudpickle.loads( _results_slice )

            print('&&& job_deploy :: slice #', str(_results_index), ' data_unpickled ::', _results_slice_unpickled)

            _final_results[_results_index] = _results_slice_unpickled

    print('&&& job_deploy :: _final_results ::')
    print(_final_results)

    return _final_results

Class Job():

    def __init__(self, input_set, work_function, work_arguments = []):

        # mandatory job arguments
        self.input_set = input_set
        self.work_function = work_function
        self.work_arguments = work_arguments

        # additional job properties
        self.compute_groups = []
        self.requires = []
        self.requirements = {}

    def exec(self):

        self.results = job_deploy(self.input_set, self.work_function, self.work_arguments, self.requires, self.compute_groups)

Class Compute():

    def __init__(self, scheduler_url):

        self.scheduler = scheduler_url

    def for(input_set, work_function, work_arguments = []):

        job = Job(input_set, work_function, work_arguments)

        return job
