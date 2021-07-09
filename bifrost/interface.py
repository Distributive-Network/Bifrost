from bifrost import node, npm

import io
import contextlib

import cloudpickle
import codecs

def dcp_install(job_scheduler = 'https://scheduler.distributed.computer'):

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

def dcp_wallet():

    wallet_output = node.run("""

    loadKeystore();

    """)

    return True

def dcp_run(job_data, job_multiplier, job_local, job_input, job_functions, job_modules, job_packages, job_imports):

    run_parameters = {
        'dcp_data': job_data,
        'dcp_multiplier': job_multiplier,
        'dcp_local': job_local,
        'python_parameters': job_input,
        'python_functions': job_functions,
        'python_modules': job_modules,
        'python_packages': job_packages,
        'python_imports': job_imports
    }

    node_output = node.run("""

    (async function(){

        async function dcpPost(myData, myFunction, myParameters, myMultiplier, myLocal) {

            const jobStartTime = Date.now();

            let jobResults = [...Array(myData.length / myMultiplier)].map(x => []);

            let jobTimings = [];

            let distributedCount = 0;

            let job = compute.for(myData, myFunction, myParameters);

            job.public.name = 'AITF : Bifrost -> Pyodide';
            
            job.requires([
                'aitf-compress/pako',
                'aitf-shard-loader/shard-loader',
            ]);

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

        let jobFunction = `async function(sliceData, sliceParameters, sliceFunctions, sliceModules, slicePackages, sliceImports) {

          try {

            const startTime = Date.now();

            progress(0);

            await console.log('Starting worker function for slice ' + sliceData.index + ' at ' + startTime + '...');

            let shardLoader = await require('shard-loader');

            progress(1);

            const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);

            let sliceOutput = sliceData.index ** 5;

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

        let jobParameters = [
            python_parameters,
            python_functions,
            python_modules,
            python_packages,
            python_imports
        ];

        jobOutput = await dcpPost(jobData, jobFunction, jobParameters, jobMultiplier, jobLocal);

    })();

    """, run_parameters)

    job_output = node_output['jobOutput']
    
    return job_output

def dcp_deploy(
        dcp_slices,
        dcp_functions = {},
        dcp_arguments = {},
        dcp_imports = [],
        dcp_packages = [],
        dcp_multiplier = 1,
        dcp_local = 0,
        dcp_python = True):

    job_slices = dcp_slices
    job_functions = dcp_functions
    job_arguments = dcp_arguments
    job_imports = dcp_imports
    job_packages = dcp_packages
    job_multiplier = dcp_multiplier
    job_local = dcp_local
    job_python = dcp_python

    def _module_writer(module_name):

        if job_python:
            module_extension = '.py'
        else:
            module_extension = '.js'

        module_filename = module_name + module_extension

        with open(module_filename, 'rb') as module:
            module_data = module.read()

        module_encoded = codecs.encode( module_data, 'base64' ).decode()

        return module_encoded

    def _pickle_jar(input_data):

        if job_python:
            data_pickled = cloudpickle.dumps( input_data )
        else:
            data_pickled = json.dumps( input_data )

        data_encoded = codecs.encode( data_pickled, 'base64' ).decode()

        return data_encoded

    job_modules = {}
    for module_name in job_imports:
        job_modules[module_name] = _module_writer(module_name)

    job_arguments = _pickle_jar(job_arguments)
    job_functions = _pickle_jar(job_functions)

    for block_index, block_slice in enumerate(job_slices):

        block_slice = _pickle_jar(block_slice)

        job_slices[block_index] = {
            'index': block_index,
            'data': block_slice }

    job_input = []

    for i in range(job_multiplier):
        job_input.extend(job_slices)

    dcp_wallet()

    job_results = dcp_run(job_input, job_multiplier, job_local, job_arguments, job_functions, job_modules, job_packages, job_imports)

    for results_index, results_slice in enumerate(job_results):

        results_slice = codecs.decode( results_slice.encode(), 'base64' )

        if job_python:
            results_slice = cloudpickle.loads( results_slice )
        else:
            results_slice = json.loads( results_slice )

        job_results[results_index] = results_slice

    return job_results

