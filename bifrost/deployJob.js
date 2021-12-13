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

        let jobFunctions = {
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
                jobFunctions.accepted = function onJobAccepted()
                {
                    console.log('Accepted: ' + job.id);
                }

                jobFunctions.complete = function onJobConsole(myEvent)
                {
                    console.log('Complete: ' + job.id);
                }

                jobFunctions.console = function onJobConsole(myConsole)
                {
                    console.log(myConsole.sliceNumber + ' : ' + myConsole.level, ' : ' + myConsole.message);
                }

                jobFunctions.error = function onJobError(myError)
                {
                    console.log(myError.sliceNumber + ' : error : ' + myError.message);
                }

                jobFunctions.readystatechange = function onJobReadyStateChange(myStateChange)
                {
                    console.log(myStateChange);
                }

                jobFunctions.result = function onJobResult(myResult)
                {
                    if (myResult.result.hasOwnProperty('output'))
                    {
                        if (jobResults[myResult.result.index].length == 0)
                        {
                            jobResults[myResult.result.index] = myResult.result.output;

                            jobTimings.push(parseInt(myResult.result.elapsed, 10));

                            let percentComputed = ((jobTimings.length / jobResults.length) * 100).toFixed(2);
                            console.log('Computed: ' + percentComputed + '%');
                            
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

                job.on('result', jobFunctions['result']);
                for ( event in dcp_events )
                {
                    if (jobFunctions[event]) job.on(event, jobFunctions[event]);
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

        job.removeEventListener('result', jobFunctions['result']);
        for ( event in dcp_events )
        {
            job.removeEventListener(event, jobFunctions[event]);
        }

        const averageSliceTime = jobTimings.reduce((a, b) => a + b) / finalResults.length;
        const totalJobTime = Date.now() - jobStartTime;

        console.log('Total Elapsed Job Time: ' + (totalJobTime / 1000).toFixed(2) + ' s');
        console.log('Mean Elapsed Worker Time Per Slice: ' + averageSliceTime + ' s');
        console.log('Mean Elapsed Client Time Per Unique Slice: ' + ((totalJobTime / 1000) / finalResults.length).toFixed(2) + ' s');
        
        return finalResults;
    }
    
    let jobFunction = require('./workFunction').workFunction;

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

    try
    {
        jobOutput = await dcpPost(jobData, jobFunction, jobParameters, jobMultiplier, jobLocal);
    }
    catch (e)
    {
        await console.log('CAUGHT NODEJS ERROR : ' + e);
    }
})();
