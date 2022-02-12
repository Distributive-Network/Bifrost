(async function()
{
    async function dcpPost(myData, workFunction, sharedArguments, myMultiplier, myLocal)
    {
        const jobStartTime = Date.now();

        let jobResults = [...Array(myData.length / myMultiplier)].map(x => []);

        let jobTimings = [];

        let compute = await require('dcp/compute');

        if (dcp_debug) console.log('DCP Client Build :', await require('dcp/build'));

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
            job.requires('pyodide.0.19.0-packages.json/packages.json.js');
            job.requires('pyodide.0.19.0-pyodide.asm.data/pyodide.asm.data.js');
            job.requires('pyodide.0.19.0-pyodide.asm.wasm/pyodide.asm.wasm.js');
            job.requires('pyodide.0.19.0-pyodide.asm.js/pyodide.asm.js.js');
            job.requires('pyodide.0.19.0-pyodide.js/pyodide.js.js');
            job.requires('pyodide.0.19.0-pyodide_py.tar/pyodide_py.tar.js');
            job.requires('pyodide.0.19.0-distutils.data/distutils.data.js');
            job.requires('pyodide.0.19.0-distutils.js/distutils.js.js');
            job.requires('0.19.0a1-pyodide.asm.js/fs');
            job.requires('0.19.0a1-fetch/fetch.js');
            job.requires('pyodide.0.19.0-cloudpickle.data/cloudpickle.data.js');
            job.requires('pyodide.0.19.0-cloudpickle.js/cloudpickle.js.js');
            for (let i = 0; i < python_packages.length; i++)
            {
                let thisPackageName = python_packages[i];
                let packageDataPath = 'pyodide.0.19.0-' + thisPackageName + '.data/' + thisPackageName + '.data.js';
                let packageJsPath = 'pyodide.0.19.0-' + thisPackageName + '.js/' + thisPackageName + '.js.js';
                job.requires(packageDataPath);
                job.requires(packageJsPath);

                let thisPackagePath = 'aitf-' + thisPackageName + '-16/' + thisPackageName;
                job.requires(thisPackagePath);
            }
        }
        else
        {
            for (let i = 0; i < python_packages.length; i++)
            {
                let thisPackageName = python_packages[i];
                job.requires(thisPackageName);
            }
        }
        job.requires('aitf-compress/pako');

        let eventFunctions = {
            accepted: () => {},
            complete: () => {},
            console: () => {},
            error: () => {},
            readystatechange: () => {},
            result: () => {},
            status: () => {},
        };

        async function dcpPromise()
        {
            return new Promise(function(resolve, reject)
            {
                eventFunctions.accepted = function onJobAccepted()
                {
                    console.log('Accepted :', job.id);
                }

                eventFunctions.complete = function onJobComplete(myComplete)
                {
                    console.log('Complete :', job.id);

                    resolve(Array.from(myComplete));
                }

                eventFunctions.console = function onJobConsole(myConsole)
                {
                    console.log(myConsole.sliceNumber + ' : ' + myConsole.level, ' : ' + myConsole.message);
                }

                eventFunctions.error = function onJobError(myError)
                {
                    console.log(myError.sliceNumber + ' : error : ' + myError.message);
                }

                eventFunctions.readystatechange = function onJobReadyStateChange(myReadyStateChange)
                {
                    console.log('State :', myReadyStateChange);
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

                            if (job.debug) console.log(myResult.result.index, ': Python Log :', myResult.result.stdout);
                        }

                        let emptyIndexArray = jobResults.filter(thisResult => thisResult.length == 0);

                        if (myMultiplier > 1) console.log('Unique Slices Remaining : ' + emptyIndexArray.length);

                        if (emptyIndexArray.length == 0)
                        {
                            resolve(jobResults);
                        }
                    }
                    else if (myResult.result.hasOwnProperty('error'))
                    {
                        console.log(myResult.result.index, ': Slice Error :', myResult.result.error);
                        console.log(myResult.result.index, ': Python Log :', myResult.result.stdout);
                    }
                    else
                    {
                        console.log('Bad Result (no "output" property) : ' + myResult);
                    }
                }

                eventFunctions.status = function onJobStatus(myStatus)
                {
                    console.log('Status :', myStatus);
                }

                for ( event in dcp_events )
                {
                    if (eventFunctions[event]) job.on(event, eventFunctions[event]);
                }

                let execResults;

                if ( myLocal > 0 )
                {
                    execResults = job.localExec(myLocal);
                }
                else
                {
                    execResults = job.exec();
                }

                execResults.then
                (
                    function execHandler(execResolved)
                    {
                        resolve(Array.from(execResolved));
                    }
                );
            });
        }

        let finalResults = await dcpPromise();

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
            python_compute_worker,
            python_pickle_function,
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
            catch (error)
            {
              await console.log('Work Function Error :', error);
              throw error;
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
    catch (error)
    {
        await console.log('Deploy Job Error :', error);
    }
})();
