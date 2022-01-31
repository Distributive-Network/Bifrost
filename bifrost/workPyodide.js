async function workFunction
(
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
    try
    {
        const startTime = Date.now();

        progress(0);

        // python files
        if (!globalThis.pyDcp) globalThis.pyDcp = {};

        // core pyodide files
        let pyFiles =
        [
          { filepath: './', filename: 'pyodide.asm.data'},
          { filepath: './', filename: 'pyodide.asm.wasm'},
          { filepath: './', filename: 'pyodide_py.tar'},
          { filepath: './', filename: 'packages.json'},
          { filepath: './', filename: 'pyodide.asm.js'},
          { filepath: './', filename: 'distutils.data'},
          { filepath: './', filename: 'distutils.js'},
          { filepath: './', filename: 'pyodide.js'},
        ];

        // supported python packages
        for (let i = 0; i < pythonPackages.length; i++)
        {
            pyFiles.push({ filepath: './', filename: pythonPackages[i] + '.data'});
            pyFiles.push({ filepath: './', filename: pythonPackages[i] + '.js'});
        }

        // download and decode any files that have not already been cached
        for (let i = 0; i < pyFiles.length; i++)
        {
            let fileKey = pyFiles[i].filepath + pyFiles[i].filename;
            if (!pyDcp[fileKey])
            {
                let fileLoader = require(pyFiles[i].filename + '.js');
                await fileLoader.download();
                pyDcp[fileKey] = await fileLoader.decode();
            }
            progress();
        }

        // python stdout
        if (!globalThis.pyLog) globalThis.pyLog = [];
        if (!globalThis.pyLogger) globalThis.pyLogger = function pyLogger(pyInput)
        {
            globalThis.pyLog.push(input);
        };
        pyLog = ['// PYTHON LOG START //'];

        // python scope
        if (!globalThis.pyScope) globalThis.pyScope = {};

        const globalKeys = Object.keys(globalThis);
        const dangerKeys = []; // anything we want to keep out of the python global scope

        // we do this to avoid serializing the dcp worker scope directly
        for (let i = 0; i < globalKeys.length; i++)
        {
            const thisKey = globalKeys[i];
            if ( !dangerKeys.includes(thisKey) && !pyScope[thisKey] )
            {
                pyScope[thisKey] = globalThis[thisKey];
            }
        }

        // initialize and load pyodide if required
        if (!globalThis.pyodide)
        {
            await require('pyodide.js.js').packages(pyDcp['./pyodide.js']);

            globalThis.pyodide = await loadPyodide
            (
                {
                    indexURL : "./",
                    jsglobals : pyScope,
                    stdout: pyLogger
                }
            );
        }

        progress();

        pyodide.globals.set('input_imports', pythonImports);
        pyodide.globals.set('input_modules', pythonModules);

        await pyodide.runPythonAsync(pythonInitWorker);

        await pyodide.runPythonAsync(sliceFunction[1]); //function.code

        progress();
        
        pyodide.globals.set('input_data', sliceData.data);
        pyodide.globals.set('input_parameters', sliceParameters);
        pyodide.globals.set('input_function', sliceFunction[0]); //function.name

        await pyodide.runPythonAsync(pythonComputeWorker);

        progress();

        let sliceOutput = pyodide.globals.get('output_data_encoded');

        pyLog.push('// PYTHON LOG STOP //');

        const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);

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
