(async function()
{
    async function dcpPost(inputSet, workFunction, sharedArguments, myMultiplier, myLocal)
    {
        const jobStartTime = Date.now();

        let jobResults = [...Array(inputSet.length / myMultiplier)].map(x => []);

        let jobTimings = [];

        let compute = await require('dcp/compute');

        if (dcp_parameters['dcp_debug']) console.log('DCP Client Build :', await require('dcp/build'));

        let kvin = (dcp_parameters['dcp_kvin']) ? require('kvin') : null;

        // bifrost can decide to force redeployments at any point during the job's runtime.
        // this can be done to save a job from failing, or to expedite its completion.
        // we are going to retain the following between any redeployments of this job:
        // -> jobStartTime : the clock is still ticking
        // -> jobResults, jobTimings : we are going to carry over any and all successfully received results
        // -> compute : resetting the API is just kinda pointless
        // -> inputSet : we're going to cut out anything we've already got jobResults for before redeploying
        // -> workFunction, sharedArguments : these haven't changed, in their content or in their necessity
        // -> myMultiplier, myLocal : we may play with the multipliers in future, but for now assume no change

        // this is the start of our redeployment zone

        let job = compute.for(inputSet, workFunction, sharedArguments);

        job.collateResults = job_parameters['job_collate'];

        if (job_parameters['job_groups'].length > 0) job.computeGroups = job_parameters['job_groups'];

        job.estimationSlices = job_parameters['job_estimation'];

        job.greedyEstimation = job_parameters['job_greedy'];

        job.public = job_parameters['job_public'];

        job.debug = job_parameters['job_debug'];

        job.requirements.discrete = false;

        // set module requirements for python job
        if (dcp_parameters['dcp_node_js'] == false)
        {
            let requiresPackages = worker_parameters['python_packages'];
            let versionNamespace = (worker_config_flags['pyodide']['wheels'] == false) ? 'pyodide' : 'pyodide-0.21.0a2';

            let pyodideShards = (worker_config_flags['pyodide']['wheels'] == false) ? null : require('./dcp/pyodide/shards.json');

            function requiresShards(pyFile)
            {
                if (pyodideShards && typeof pyodideShards['packages'] !== 'undefined' && typeof pyodideShards['packages'][pyFile] !== 'undefined')
                {
                    let fileShards = pyodideShards.packages[pyFile];

                    for (let i = 0; i < fileShards.length; i++)
                    {
                        let thisFileShard = fileShards[i];
                        job.requires(versionNamespace + '-' + thisFileShard.toLowerCase() + '/' + thisFileShard);
                    }
                    return fileShards.length; // number of file shard packages that are associated with this python file
                }
                else
                {
                    return 0; // file shard packages are not being used, or this package is not on the list
                }
            }

            job.requires(versionNamespace + '-pyodide.asm.data/pyodide.asm.data.js');
            job.requires(versionNamespace + '-pyodide.asm.wasm/pyodide.asm.wasm.js');
            job.requires(versionNamespace + '-pyodide_py.tar/pyodide_py.tar.js');
            job.requires(versionNamespace + '-pyodide.asm.js/pyodide.asm.js.js');
            job.requires(versionNamespace + '-pyodide.js/pyodide.js.js');

            requiresShards('pyodide.asm.wasm');

            if (worker_config_flags['pyodide']['wheels'] == false)
            {
                job.requires(versionNamespace + '-distutils.data/distutils.data.js');
                job.requires(versionNamespace + '-distutils.js/distutils.js.js');
                job.requires(versionNamespace + '-packages.json/packages.json.js');
            }
            else
            {
                job.requires(versionNamespace + '-package.json/package.json.js');
                job.requires(versionNamespace + '-distutils.tar/distutils.tar.js');
                job.requires(versionNamespace + '-repodata.json/repodata.json.js');
            }

            let pyodideDepends = (worker_config_flags['pyodide']['wheels'] == false) ? require('./dcp/pyodide/packages.json') : require('./dcp/pyodide/repodata.json');

            let pyodideRequireFiles = pyodideDepends.packages;
            let pyodideRequireFilesKeys = Object.keys(pyodideRequireFiles);
            let pyodideRequireNames = {};
            for (let i = 0; i < pyodideRequireFilesKeys.length; i++)
            {
                const thatKey = pyodideRequireFilesKeys[i];
                const thisKey = pyodideRequireFiles[thatKey]['name'];
                pyodideRequireNames[thisKey] = thatKey;
            }

            function addToJobRequires(pyFile, requestAncestors = ['distutils'])
            {
                let packageKey = pyodideRequireNames[pyFile] || pyFile;
                let packageInfo = pyodideRequireFiles[packageKey];
                let packageName = ( packageInfo && typeof packageInfo['name'] !== 'undefined' ) ? packageInfo['name'] : pyFile;
                let packageDepends = ( packageInfo && typeof packageInfo['depends'] !== 'undefined' ) ? packageInfo['depends'] : [];

                requestAncestors.push(packageKey);

                for (dependency of packageDepends)
                {
                    if (!requestAncestors.includes(dependency)) addToJobRequires(dependency, requestAncestors);
                }

                if (worker_config_flags['pyodide']['wheels'] == false)
                {
                    const packageFileDataPath = versionNamespace + '-' + packageName.toLowerCase() + '.data/';
                    const packageFileData = packageName + '.data.js';
                    job.requires(packageFileDataPath + packageFileData);

                    const packageFileJsPath = versionNamespace + '-' + packageName.toLowerCase() + '.js/';
                    const packageFileJs = packageName + '.js.js';
                    job.requires(packageFileJsPath + packageFileJs);
                }
                else
                {
                    const packageNameFull = ( packageInfo && typeof packageInfo['file_name'] !== 'undefined' ) ? packageInfo['file_name'] : packageName;
                    const packageFileJsPath = versionNamespace + '-' + packageNameFull.toLowerCase() + '/';
                    const packageFileJs = packageNameFull + '.js';
                    job.requires(packageFileJsPath + packageFileJs);
                    requiresShards(packageNameFull); // check if this file is broken into a package for each shard, and add to job.requires accordingly
                }
            }

            if (worker_config_flags['pyodide']['wheels'] == false && worker_config_flags['cloudpickle'] == true) requiresPackages.push('cloudpickle');

            for (let i = 0; i < worker_parameters['python_packages'].length; i++)
            {
                let thisPackageName = worker_parameters['python_packages'][i];
                addToJobRequires(thisPackageName);
            }
        }
        else
        {
            for (let i = 0; i < worker_parameters['python_packages'].length; i++)
            {
                let thisPackageName = worker_parameters['python_packages'][i];
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
        };

        let jobResultInterval;
        let resultIntervalFunction = function()
        {
            job.results.fetch( null, emitEvents = true ); // TODO : configurable flags
            const fetchResultCount = Array.from(job.results).length;
            if ( job.debug ) console.log('Job Result Fetch Count', ':', fetchResultCount, ':', Date.now());
            // TODO : support for (myMultiplier > 1)
            if ( !dcp_parameters['dcp_kvin'] && fetchResultCount >= inputSet.length ) resolve({ bifrostResultHandle: job.results });
        }

        async function dcpPromise()
        {
            return new Promise(function(resolve, reject)
            {
                eventFunctions.accepted = function onJobAccepted()
                {
                    console.log('Accepted :', job.id);

                    jobId = job.id;

                    // TODO : make contingent on certain conditions or flags
                    // TODO : configurable result threshold for resolving
                    // TODO : configurable timer value, flag for interval vs single-shot timeout
                    jobResultInterval = setInterval(resultIntervalFunction, 60000);
                }

                eventFunctions.complete = function onJobComplete(myComplete)
                {
                    console.log('Complete :', job.id);

                    const completeResultCount = Array.from(myComplete).length;
                    // TODO : support for (myMultiplier > 1)
                    if ( completeResultCount >= inputSet.length ) resolve({ bifrostResultHandle: myComplete });
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
                    let kvinMimeString = 'data:application/x-kvin,';

                    if (dcp_parameters['dcp_kvin'] && typeof myResult.result == 'string' && myResult.result.includes(kvinMimeString)) myResult.result = kvin.deserialize(myResult.result.slice(kvinMimeString.length));

                    if (myResult.result.hasOwnProperty('output'))
                    {
                        if (jobResults[myResult.result.index].length == 0)
                        {
                            jobResults[myResult.result.index] = myResult.result.output;

                            jobTimings.push(parseInt(myResult.result.elapsed, 10));

                            let percentComputed = ((jobTimings.length / jobResults.length) * 100).toFixed(2);

                            console.log('Computed : ' + percentComputed + '%');

                            if ( (dcp_parameters['dcp_node_js'] == false) && job.debug ) console.log(myResult.result.index, ': Python Log :', myResult.result.stdout);
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
                        if (dcp_parameters['dcp_node_js'] == false) console.log(myResult.result.index, ': Python Log :', myResult.result.stdout);
                    }
                    else
                    {
                        console.log('Bad Result (no "output" property) :', myResult);
                    }

                    clearInterval(jobResultInterval);
                    jobResultInterval = setInterval(resultIntervalFunction, 60000);
                }

                for ( event in dcp_parameters['dcp_events'] )
                {
                    if (dcp_parameters['dcp_events'][event] && eventFunctions[event]) job.on(event, eventFunctions[event]);
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

                function execHandler( promiseExec )
                {
                    const execResultCount = Array.from(promiseExec).length;
                    // TODO : support for (myMultiplier > 1)
                    if ( execResultCount >= inputSet.length ) resolve({ bifrostResultHandle: promiseExec });
                }

                execResults.then( execHandler );
            });
        }

        let dcpPromiseResults = await dcpPromise();

        clearInterval(jobResultInterval);

        for ( event in dcp_parameters['dcp_events'] )
        {
            if (dcp_parameters['dcp_events'][event] && eventFunctions[event]) job.removeEventListener(event, eventFunctions[event]);
        }

        if ( dcpPromiseResults['bifrostResultHandle'] )
        {
            let handleResults = Array.from(dcpPromiseResults['bifrostResultHandle']);
            for ( let i = 0; i < handleResults.length; i++)
            {
                let myResult = handleResults[i];

                if (myResult.hasOwnProperty('output'))
                {
                    if (jobResults[myResult.index].length == 0)
                    {
                        jobResults[myResult.index] = myResult.output;

                        jobTimings.push(parseInt(myResult.elapsed, 10));
                    }
                }
                else if (myResult.hasOwnProperty('error'))
                {
                    console.log(myResult.index, ': Slice Error :', myResult.error);
                    if (dcp_parameters['dcp_node_js'] == false) console.log(myResult.index, ': Python Log :', myResult.stdout);
                }
                else
                {
                    console.log('Bad Result (no "output" property) : ' + myResult);
                }
            }
        }

        if (!jobId) jobId = job.id;

        // this the end of the redeployment zone

        // nothing after this point should ever be called more than once as part of the same user-submitted job.
        // time metrics especially must account for all redeployment attempts, and can never reset in between.

        if (dcp_parameters['dcp_show_timings'])
        {
            const averageSliceTime = jobTimings.reduce((a, b) => a + b) / jobResults.length;
            const totalJobTime = Date.now() - jobStartTime;

            console.log('Total Elapsed Job Time: ' + (totalJobTime / 1000).toFixed(2) + ' s');
            console.log('Mean Elapsed Worker Time Per Slice: ' + averageSliceTime + ' s');
            console.log('Mean Elapsed Client Time Per Unique Slice: ' + ((totalJobTime / 1000) / jobResults.length).toFixed(2) + ' s');
        }
        
        return jobResults;
    }

    let jobData = dcp_parameters['dcp_data'];
    let jobMultiplier = dcp_parameters['dcp_multiplier'];
    let jobLocal = dcp_parameters['dcp_local'];

    let workFunction;
    let sharedArguments;

    if (dcp_parameters['dcp_node_js'] == false)
    {
        workFunction = dcp_parameters['dcp_wrapper'];

        sharedArguments = [
            worker_parameters,
            worker_config_flags,
        ];
    }
    else
    {
        workFunction = `async function(sliceData, sliceParameters)
        {
            try
            {
                const startTime = Date.now();
                progress(0);
                let sliceFunction = ${worker_parameters['slice_workload']['workload_function']};
                let sliceOutput = await sliceFunction(sliceData.data, ...sliceParameters);
                progress(1);
                const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);
                return {
                    output: sliceOutput,
                    index: sliceData.index,
                    elapsed: stopTime,
                };
            }
            catch (error)
            {
              console.log('Work Function Error (Slice ', sliceData.index, ') :', error);
              throw error;
            }
        }`;

        let jobArguments = [];
        nodeSharedArguments.forEach(x => {
            let myItem = (Object.prototype.toString.call(x) == '[object Object]') ? Object.fromEntries(Object.entries(x)) : x;
            jobArguments.push(myItem);
            return [];
        });
        sharedArguments = [ jobArguments ];
    }

    let inputSet = [];
    jobData.forEach(x => {
        let myItem = Object.fromEntries(Object.entries(x));
        inputSet.push(myItem);
        return [];
    });

    jobId = null;

    try
    {
        jobOutput = await dcpPost(inputSet, workFunction, sharedArguments, jobMultiplier, jobLocal);
    }
    catch (error)
    {
        console.log('Deploy Job Error :', error);
    }
})();
