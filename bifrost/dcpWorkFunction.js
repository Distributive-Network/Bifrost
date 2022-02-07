async function workFunction(
    sliceData,// input slice, primary arg to user-provided function
    sliceParameters,// shared parameters, secondary args to user-provided function
    sliceFunction,// user-provided function to be run on input slice
    pythonModules,// user-provided python module scripts to be imported into environment
    pythonPackages,// dcp-provided pyodidie packages to be loaded into environment
    pythonImports,// user-provided list of python import names to be imported into environment
    pythonInitWorker,// dcp-provided python function to initialize environment
    pythonComputeWorker,// dpc-provided python function to handle work function
    pythonPickleFunction,// flag which indicates that the work function is a cloudpickle object
)
{
  const startTime = Date.now();

  try
  {
    progress();

    class PyodideXMLHttpRequest
    {
        method;
        async;
        url;
        body;

        response;
        responseType;
        onload;
        onerror;

        constructor()
        {
            this.method = null;
            this.async = null;
            this.url = null;
            this.body = null;

            this.response = null;
            this.responseType = null;
            this.onload = null;
            this.onerror = null;
        }

        open(method, url, async = true, user = null, password = null)
        {
            this.method = method;
            this.url = url;
            this.async = async;

            if (pyDcp[this.url])
            {
              this.response = pyDcp[this.url];
            }
            else
            {
              throw('Missing file:', input, '(xhr.open)');
            }
            return 1;
        }

        send(body = null)
        {
            return 1;
        }
    }
    globalThis.XMLHttpRequest = PyodideXMLHttpRequest;

    let process = {};
    globalThis.process = process;

    let crypto = {
        getRandomValues: function(typedArray)
        {
            return typedArray.map(x => Math.floor(Math.random() * 256));
        }
    };
    globalThis.crypto = crypto;

    var fs = await require("fs");
    fs.readFile = async function readFile(path, callback)
    {
        return await new Promise((resolve, reject) => {
            try
            {
                if ( pyDcp[path] )
                {
                  resolve(callback( null, pyDcp[path] ));
                }
                else
                {
                  reject('Missing file:', path);
                }
            }
            catch(myError)
            {
                reject(myError);
            };
        });
    }

    async function pyodideFetch(input, init = {})
    {
      async function fetchArrayBuffer()
      {
        if (pyDcp[input].buffer)
        {
          return pyDcp[input].buffer;
        }
        else
        {
          throw('Missing file:', input, '(fetch.arrayBuffer)');
        }
      }
      async function fetchJson()
      {
        if (pyDcp[input])
        {
          return JSON.parse(pyDcp[input]);
        }
        else
        {
          throw('Missing file:', input, '(fetch.json)');
        }
      }
      let fetchResponseFunctions = {
          arrayBuffer: fetchArrayBuffer,
          json: fetchJson,
          ok: true,
      }
      return fetchResponseFunctions;
    }
    globalThis.fetch = pyodideFetch;

    globalThis.AbortController = () => {};
    globalThis.FormData = () => {};
    globalThis.URLSearchParams = () => {};

    async function importScripts(...args)
    {
        for (let i = 0; i < args.length; i++)
        {
            let thisArg = args[i];

            if (pyDcp[thisArg])
            {
                let importLoader = require('pyodide.js.js');
                importLoader.packages(pyDcp[thisArg]);
            }
            else
            {
                throw('Missing file:', thisArg, '(importScripts)');
            }
        }
    }
    globalThis.importScripts = importScripts;

    globalThis.WebAssembly.instantiateStreaming = null;

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
      { filepath: './', filename: 'cloudpickle.data'},
      { filepath: './', filename: 'cloudpickle.js'},
    ];

    for (let i = 0; i < pythonPackages.length; i++)
    {
      const packageName = pythonPackages[i];
      const packageFileData = packageName + '.data';
      const packageFileJs = packageName + '.js';
      pyFiles.push({ filepath: './', filename: packageFileData });
      pyFiles.push({ filepath: './', filename: packageFileJs });
    }

    if (!globalThis.pyDcp) globalThis.pyDcp = {};

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

    let pyodideLoader = require('pyodide.js.js');
    await pyodideLoader.packages(pyDcp['./pyodide.js']);
    progress();

    if (!globalThis.pyScope) globalThis.pyScope = {};

    const globalKeys = Object.keys(globalThis);
    for (let i = 0; i < globalKeys.length; i++)
    {
        let thisKey = globalKeys[i];
        if ( !pyScope[thisKey] ) pyScope[thisKey] = globalThis[thisKey];
    }

    if (!globalThis.pyLog) globalThis.pyLog = [];
    pyLog = ['// PYTHON LOG START //'];

    if (!globalThis.pyLogger) globalThis.pyLogger = function pyLogger(input)
    {
        globalThis.pyLog.push(input);
    }

    if (!globalThis.pyodide) globalThis.pyodide = await loadPyodide
    (
      {
        indexURL : "./",
        jsglobals : pyScope,
        stdout: pyLogger,
        stderr: pyLogger,
      }
    );
    progress();

    let packagesKeys = Object.keys(pyodide.loadedPackages);

    if ( Object.keys(pyodide.loadedPackages).indexOf('cloudpickle') === -1 ) await pyodide.loadPackage(['cloudpickle']);

    progress();

    for (let i = 0; i < pythonPackages.length; i++)
    {
      if ( Object.keys(pyodide.loadedPackages).indexOf(pythonPackages[i]) === -1 ) await pyodide.loadPackage([pythonPackages[i]]);

      progress();
    }

    pyodide.globals.set('input_imports', pythonImports);
    pyodide.globals.set('input_modules', pythonModules);

    await pyodide.runPython(pythonInitWorker);

    progress();

    pyodide.globals.set('pickle_function', pythonPickleFunction);

    if (pythonPickleFunction)
    {
        pyodide.globals.set('input_function', sliceFunction);
    }
    else
    {
        await pyodide.runPython(sliceFunction['code']);
        pyodide.globals.set('input_function', sliceFunction['name']);
    }

    pyodide.globals.set('input_data', sliceData['data']);
    pyodide.globals.set('input_parameters', sliceParameters);

    await pyodide.runPython(pythonComputeWorker);

    progress(1);

    pyLog.push('// PYTHON LOG STOP //');

    let sliceOutput = pyodide.globals.get('output_data');

    const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);

    let resultObject = {
        output: sliceOutput,
        index: sliceData['index'],
        elapsed: stopTime,
        stdout: pyLog,
    };

    return resultObject;
  }
  catch(error)
  {
    pyLog.push('// PYTHON LOG ERROR //');

    const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);

    let errorObject = {
        error: error,
        index: sliceData['index'],
        elapsed: stopTime,
        stdout: pyLog,
    };

    return errorObject;
  }
}
