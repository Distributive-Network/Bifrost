js_deploy_job = """
(async function()
{
    async function dcpPost(myData, workFunction, sharedArguments, myMultiplier, myLocal)
    {
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

        job.debug = dcp_debug;

        job.requirements = dcp_requirements;

        // set module requirements for python job
        if (dcp_node_js == false)
        {
            for (let i = 0; i < python_packages.length; i++)
            {
                let thisPackageName = python_packages[i];
                let thisPackagePath = 'aitf-' + thisPackageName + '-16/' + thisPackageName;
                job.requires(thisPackagePath);
            }
            job.requires('aitf-pyodide-16/pyodide');
            job.requires('aitf-cloudpickle-16/cloudpickle');
        }
        job.requires('aitf-compress/pako');

        let eventFunctions = {
            accepted: () => {},
            complete: () => {},
            console: () => {},
            error: () => {},
            readystatechange: () => {},
            result: () => {}
        };

        async function dcpPromise()
        {
            return new Promise(function(resolve, reject)
            {
                eventFunctions.accepted = function onJobAccepted()
                {
                    console.log('Accepted: ' + job.id);
                }

                eventFunctions.complete = function onJobConsole(myEvent)
                {
                    console.log('Complete: ' + job.id);
                }

                eventFunctions.console = function onJobConsole(myConsole)
                {
                    console.log(myConsole.sliceNumber + ' : ' + myConsole.level, ' : ' + myConsole.message);
                }

                eventFunctions.error = function onJobError(myError)
                {
                    console.log(myError.sliceNumber + ' : error : ' + myError.message);
                }

                eventFunctions.readystatechange = function onJobReadyStateChange(myStateChange)
                {
                    console.log(myStateChange);
                }

                eventFunctions.result = function onJobResult(myResult)
                {
                    if (myResult.result.hasOwnProperty('output'))
                    {
                        if (jobResults[myResult.result.index].length == 0)
                        {
                            jobResults[myResult.result.index] = myResult.result.output;

                            jobTimings.push(parseInt(myResult.result.elapsed, 10));

                            let percentComputed = ((jobTimings.length / jobResults.length) * 100).toFixed(2);
                            console.log('Computed : ' + percentComputed + '%');
                            
                            console.log('Result :', myResult.result);
                        }

                        let emptyIndexArray = jobResults.filter(thisResult => thisResult.length == 0);

                        console.log('Unique Slices Remaining : ' + emptyIndexArray.length);

                        if (emptyIndexArray.length == 0)
                        {

                            resolve(jobResults);
                        }
                    }
                    else if (myResult.result.hasOwnProperty(''))
                    {
                        // remote url result || remote null result
                    }
                    else
                    {
                        console.log('Bad Result : ' + myResult);
                    }
                }

                job.on('result', eventFunctions['result']);
                for ( event in dcp_events )
                {
                    if (eventFunctions[event]) job.on(event, eventFunctions[event]);
                }

                if ( myLocal > 0 )
                {
                    job.localExec(myLocal);
                }
                else
                {
                    job.exec();
                }
            });
        }

        let finalResults = await dcpPromise();

        job.removeEventListener('result', eventFunctions['result']);
        for ( event in dcp_events )
        {
            job.removeEventListener(event, eventFunctions[event]);
        }

        const averageSliceTime = jobTimings.reduce((a, b) => a + b) / finalResults.length;
        const totalJobTime = Date.now() - jobStartTime;

        console.log('Total Elapsed Job Time: ' + (totalJobTime / 1000).toFixed(2) + ' s');
        console.log('Mean Elapsed Worker Time Per Slice: ' + averageSliceTime + ' s');
        console.log('Mean Elapsed Client Time Per Unique Slice: ' + ((totalJobTime / 1000) / finalResults.length).toFixed(2) + ' s');
        
        return finalResults;
    }

    let jobData = dcp_data;
    let jobMultiplier = dcp_multiplier;
    let jobLocal = dcp_local;

    let jobFunction;
    let jobParameters;
    if (dcp_node_js == false)
    {
        jobFunction = deploy_function;

        jobParameters = [
            dcp_parameters,
            dcp_function,
            python_modules,
            python_packages,
            python_imports,
            python_init_worker,
            python_compute_worker
        ];
    }
    else
    {
        jobFunction = `async function(sliceData, sliceParameters)
        {
            try
            {
                const startTime = Date.now();
                progress(0);
                await console.log('Starting worker function for slice ' + sliceData.index + ' at ' + startTime + '...');
                let sliceFunction = ${dcp_function};
                let sliceOutput = await sliceFunction(sliceData.data, ...sliceParameters);
                progress(1);
                const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);
                return {
                    output: sliceOutput,
                    index: sliceData.index,
                    elapsed: stopTime
                };
            }
            catch (e)
            {
              await console.log(e);
              throw e;
            }
        }`;

        jobParameters = [
            dcp_parameters,
        ];
    }

    try
    {
        jobOutput = await dcpPost(jobData, jobFunction, jobParameters, jobMultiplier, jobLocal);
    }
    catch (e)
    {
        await console.log('CAUGHT NODEJS ERROR : ' + e);
    }
})();
"""
