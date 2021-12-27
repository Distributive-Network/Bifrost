js_work_function = """
async function workFunction(
    sliceData,// input slice, primary arg to user-provided function
    sliceParameters,// shared parameters, secondary args to user-provided function
    sliceFunction,// user-provided function to be run on input slice
    pythonModules = null,// user-provided python module scripts to be imported into environment
    pythonPackages = null,// dcp-provided pyodidie packages to be loaded into environment
    pythonImports = null,// user-provided list of python import names to be imported into environment
    pythonInitWorker = null,// dcp-provided python function to initialize environment
    pythonComputeWorker = null,// dpc-provided python function to handle work function
)
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

        await pyodide.runPythonAsync(sliceFunction[1]); //function.code
        
        pyodide.globals.set('input_data', sliceData.data);
        pyodide.globals.set('input_parameters', sliceParameters);
        pyodide.globals.set('input_function', sliceFunction[0]); //function.name

        await pyodide.runPythonAsync(pythonComputeWorker);

        progress();

        let sliceOutput = pyodide.globals.get('output_data');

        const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);

        pythonModules = null;
        pythonPackages = null;
        pythonImports = null;        
        sliceParameters = null;
        pythonFunction = null;
        sliceData.data = null;
        
        progress(1);

        let resultObject = {
            output: sliceOutput,
            index: sliceData.index,
            elapsed: stopTime,
        };

        return resultObject;        
    }
    catch (e)
    {
        throw(e);
    }
}
"""
