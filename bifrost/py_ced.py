# MODULES

# python standard library
import codecs
import contextlib
import io
import json
import os
import zlib

# pypi modules
import cloudpickle

# PROGRAM

#TODO: make CED a subclass of Bifrost?
#TODO: make Bifrost an actual class?

class CrazyEddieDrive():

    def __init__(self, npm, node):
        self.npm = npm
        self.node = node

    def dcp_install(self, job_scheduler = 'https://scheduler.distributed.computer'):

        def _npm_checker(package_name):

            npm_io = io.StringIO()
            with contextlib.redirect_stdout(npm_io):
                self.npm.list_modules(package_name)
            npm_check = npm_io.getvalue()

            if '(empty)' in npm_check:
                self.npm.install(package_name, '--silent')

        for name in ['dcp-client']:
            _npm_checker(name)

        install_parameters = {
            'dcp_scheduler': job_scheduler
        }

        install_output = self.node.run("""

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

    def dcp_wallet(self):

        wallet_output = self.node.run("""

        loadKeystore();

        """)

        return True

    def python_job(
            self,
            job_data,
            job_multiplier,
            job_local,
            job_input,
            job_functions,
            job_modules,
            job_packages):

        run_parameters = {
            'dcp_data': job_data,
            'dcp_multiplier': job_multiplier,
            'dcp_local': job_local,
            'dcp_parameters': job_input,
            'dcp_functions': job_functions,
            'dcp_modules': job_modules,
            'dcp_packages': job_packages
        }

        python_output = self.node.run("""

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

        job_output = python_output['jobOutput']
        
        return job_output

    def nodejs_job(
            self,
            job_data,
            job_function,
            job_multiplier,
            job_local,
            job_input,
            job_packages,
            job_groups):

        run_parameters = {
            'dcp_data': job_data,
            'dcp_function': job_function,
            'dcp_multiplier': job_multiplier,
            'dcp_local': job_local,
            'dcp_parameters': job_input,
            'dcp_packages': job_packages,
            'dcp_groups': job_groups
        }

        node_output = self.node.run("""

        (async function(){

            async function dcpPost(myData, myFunction, myParameters, myMultiplier, myLocal, myRequires, myGroups) {

                const jobStartTime = Date.now();

                let jobResults = [...Array(myData.length / myMultiplier)].map(x => []);

                let jobTimings = [];

                let distributedCount = 0;

                let job = compute.for(myData, myFunction, myParameters);

                job.public.name = 'AITF : Deployed From Python';
                
                job.requires(myRequires);

                if (myGroups.length > 0) job.computeGroups = myGroups;
                
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
            let jobGroups = dcp_groups;

            let jobParameters = dcp_parameters;

            let jobRequires = [];

            for (let i = 0; i < dcp_packages.length; i++) {

                jobRequires.push(dcp_packages[i]);
            }

            jobOutput = await dcpPost(jobData, jobFunction, jobParameters, jobMultiplier, jobLocal, jobRequires, jobGroups);

        })();

        """, run_parameters)

        job_output = node_output['jobOutput']
        
        return job_output

    def dcp_deploy(
            self,
            dcp_slices,
            dcp_function,
            dcp_arguments = [],
            dcp_packages = [],
            dcp_groups = [],
            dcp_multiplier = 1,
            dcp_local = 0,
            dcp_python = False):

        job_slices = dcp_slices
        job_function = dcp_function
        job_arguments = dcp_arguments
        job_packages = dcp_packages
        job_multiplier = dcp_multiplier
        job_local = dcp_local
        job_python = dcp_python
        job_groups = dcp_groups
        
        def _pickle_jar(input_data):

            if job_python:
                data_pickled = data_pickled #data_pickled = cloudpickle.dumps( input_data )
            else:
                data_pickled = json.dumps( input_data )

            #data_pickled = codecs.encode( data_pickled, 'base64' ).decode()

            return data_pickled

        #job_arguments = _pickle_jar(job_arguments)

        for block_index, block_slice in enumerate(job_slices):

            #block_slice = _pickle_jar(block_slice)

            job_slices[block_index] = {
                'index': block_index,
                'data': block_slice }

        job_input = []

        for i in range(job_multiplier):
            job_input.extend(job_slices)

        #random.shuffle(job_input)

        if job_python:
            job_results = False
        else:
            job_results = nodejs_job(
                    job_input,
                    job_function,
                    job_multiplier,
                    job_local,
                    job_arguments,
                    job_packages,
                    job_groups)

        for results_index, results_slice in enumerate(job_results):

            #results_slice = codecs.decode( results_slice.encode(), 'base64' )

            #if job_python:
            #    results_slice = cloudpickle.loads( results_slice )
            #else:
            #    results_slice = json.loads( results_slice )

            job_results[results_index] = results_slice

        return job_results
