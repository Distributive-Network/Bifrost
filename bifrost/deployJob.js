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

        job.requirements.discrete = dcp_discrete;

        // set module requirements for python job
        job.requires('aitf-compress/pako');
        job.requires('aitf-pyodide-16/pyodide');
        job.requires('aitf-cloudpickle-16/cloudpickle');

        for (let i = 0; i < python_packages.length; i++)
        {
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
    
    let jobFunction = `async function(pythonData, pythonParameters, pythonFunction, pythonModules, pythonPackages, pythonImports, pythonInitWorker, pythonComputeWorker)
    {
        const providePackageFile = async function providePackageFile(packageNameArray)
        {
            return await new Promise((resolve, reject) => {

                try
                {
                    module.provide(packageNameArray, () => {
                        resolve();
                    });

                }
                catch(myError)
                {
                    reject(myError);
                };
            });
        };

        const getShardCount = async function getShardCount(packageName)
        {
            const entryPath = 'aitf-' + packageName + '-16/' + packageName;

            await providePackageFile([entryPath]);

            const shardEntry = await require(packageName);

            const shardCount = shardEntry.PACKAGE_SHARDS;

            return shardCount;
        };

        const downloadShards = async function downloadShards(packageName)
        {
            progress();

            let shardCount = await getShardCount(packageName);

            for (let i = 0; i < shardCount; i++)
            {
                const shardName = packageName + '-shard-' + i;
                const shardPath = 'aitf-' + packageName + '-16/' + shardName;

                await providePackageFile([shardPath]);

                progress();
            }

            return true;
        };

        const decodeShards = async function decodeShards(packageName)
        {

            async function _loadShardCount(myPackageName)
            {
                const thisPackage = await require(myPackageName);

                return thisPackage.PACKAGE_SHARDS;
            }
            
            async function _loadShardData(myShardName)
            {
                const thisPackage = await require(myShardName);

                return thisPackage.SHARD_DATA;
            }

            async function _loadBinary(base64String)
            {
                let binaryString = await atob(base64String);

                const binaryLength = binaryString.length;

                let binaryArray = new Uint8Array(binaryLength);

                for(let i = 0; i < binaryLength; i++)
                {
                  binaryArray[i] = binaryString.charCodeAt(i);
                }

                return binaryArray;
            }

            function _arrayToString(myArray)
            {
                let myString = '';

                for (let i = 0; i < myArray.length; i++)
                {
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

            for (let i = 0; i < shardCount; i++)
            {
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

            for (let i = 0; i < shardCount; i++)
            {
                const shardStart = i * stringChunkLength;

                let stringShardData = inflatorOutput.slice(shardStart, shardStart + stringChunkLength);

                let inflateString = '';

                const stringCharLimit = 9999;

                for (let j = 0; j < Math.ceil(stringShardData.length / stringCharLimit); j++)
                {
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

        const initializePyodide = async function initializePyodide(packageString)
        {
            eval(packageString);

            await languagePluginLoader;

            self.pyodide._module.checkABI = () => { return true };

            return true;
        };

        const initializePackage = async function initializePackage(packageString)
        {
            let packageFunction = await new Function(packageString);

            await packageFunction();

            return true;
        };

        const deshardPackage = async function deshardPackage(packageName, newPackage = true)
        {
            if (newPackage)
            {
                await downloadShards(packageName);
            }

            let packageString = await decodeShards(packageName);

            if (packageName == 'pyodide')
            {
                await initializePyodide(packageString);
            }
            else
            {
                await initializePackage(packageString);
            }

            //packageString = null;

            return true;
        };

        const setupPython = async function setupPython(packageList = [])
        {
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

        try
        {
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

            for (i = 0; i < pythonPackageCount; i++)
            {
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

            return
            {
                output: pyodide.globals.get('output_data'),
                index: pythonData.index,
                elapsed: stopTime
            };
        }
        catch (e)
        {
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

    try
    {
        jobOutput = await dcpPost(jobData, jobFunction, jobParameters, jobMultiplier, jobLocal);
    }
    catch (e)
    {
        await console.log('CAUGHT NODEJS ERROR : ' + e);
    }
})();
