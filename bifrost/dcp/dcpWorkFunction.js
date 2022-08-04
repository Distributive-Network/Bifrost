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
    pythonPickleArguments,// flag which indicates that the shared arguments are a cloudpickle object
    pythonPickleInput,// flag which indicates that the input slice is a cloudpickle object
    pythonPickleOutput,// flag which indicates that the output slice should be a cloudpickle object
    pythonEncodeArguments,// flag which indicates that the shared arguments are base64 encoded strings
    pythonEncodeInput,// flag which indicates that the input slice is a base64 encoded string
    pythonEncodeOutput,// flag which indicates that the output slice should be a base64 encoded string
    pythonCompressFunction,// flag which indicates that the work function has been compressed
    pythonCompressArguments,// flag which indicates that the shared arguments have been compressed
    pythonCompressInput,// flag which indicates that the input slice has been compressed
    pythonCompressOutput,// flag which indicates that the output slice should be compressed
    pythonColabPickling,// flag which indicates that all pickling was done in a colab without cloudpickle
    pythonPyodideWheels = false,// indicates a Pyodide version greater than 20, informing the initialization steps
)
{
  const startTime = Date.now();

  try
  {
    progress();

    if (typeof location !== 'undefined')
    {
      if (pythonPyodideWheels)
      {
          location = globalThis.location = {
              href: 'https://portal.distributed.computer/dcp-client/libexec/sandbox/',
              hostname: 'portal.distributed.computer',
              pathname: '/dcp-client/libexec/sandbox/',
              protocol: 'https:',
              toString: function(){ return globalThis.location.href },
          };
      }
      else
      {
        location.href = './';
        location.pathname = '';
      }
    }

    class PyodideXMLHttpRequest
    {
        method;
        async;
        url;
        body;

        response;
        responseURL;
        responseType;
        onload;
        onerror;
        onprogress;

        constructor()
        {
            this.method = null;
            this.async = null;
            this.url = null;
            this.body = null;

            this.response = null;
            this.responseURL = null;
            this.responseType = null;
            this.onload = null;
            this.onerror = null;
            this.onprogress = null;

            this.status = null;
            this.statusText = null;
        }

        open(method, url, async = true, user = null, password = null)
        {
            this.method = method;
            this.url = url;
            this.async = async;
        }

        send(body = null)
        {
            if (pyDcp[this.url])
            {
              this.response = pyDcp[this.url].buffer;
            }
            else
            {
              throw('Missing file:', input, '(xhr.open)');
            }

            this.status = 200;

            this.onload();
        }
    }
    globalThis.XMLHttpRequest = PyodideXMLHttpRequest;

    let crypto = {
        getRandomValues: function(typedArray)
        {
            return typedArray.map(x => Math.floor(Math.random() * 256));
        }
    };
    globalThis.crypto = crypto;

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

    let pyPath = pythonPyodideWheels ? '/' : './';

    let pyFiles =
    [
      { filepath: pyPath, filename: 'pyodide.asm.data'},
      { filepath: pyPath', filename: 'pyodide.asm.wasm'},
      { filepath: pyPath, filename: 'pyodide_py.tar'},
      { filepath: pyPath, filename: 'packages.json'},
      { filepath: pyPath, filename: 'pyodide.asm.js'},
      { filepath: pyPath, filename: 'pyodide.js'},
      { filepath: pyPath, filename: 'cloudpickle.data'},
      { filepath: pyPath, filename: 'cloudpickle.js'},
    ];

    if (pythonPyodideWheels)
    {
        pyFiles.push({ filepath: pyPath, filename: 'package.json'});
        pyFiles.push({ filepath: pyPath, filename: 'distutils.tar'});
    }
    else
    {
        pyFiles.push({ filepath: pyPath, filename: 'distutils.data'});
        pyFiles.push({ filepath: pyPath, filename: 'distutils.js'});
    }

    for (let i = 0; i < pythonPackages.length; i++)
    {
      const packageName = pythonPackages[i];
      if ( packageName == 'scipy' )
      {
          pyFiles.push({ filepath: pyPath, filename: 'CLAPACK.data' });
          pyFiles.push({ filepath: pyPath, filename: 'CLAPACK.js' });
      }
      const packageFileData = packageName + '.data';
      const packageFileJs = packageName + '.js';
      pyFiles.push({ filepath: pyPath, filename: packageFileData });
      pyFiles.push({ filepath: pyPath, filename: packageFileJs });
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

            // source maps are referenced in the last line of some js files; we want to strip these urls out, as the source maps will not be available
            if (fileLoader.PACKAGE_FORMAT == 'string' && pyFiles[i].filename.includes('.js') && typeof pyDcp[fileKey] == 'string')
            {
                let sourceMappingIndex = pyDcp[fileKey].indexOf('//' + '#' + ' ' + 'sourceMappingURL'); // break up the string to avoid a potential resonance cascade
                if (sourceMappingIndex != -1) pyDcp[fileKey] = pyDcp[fileKey].slice(0, sourceMappingIndex);
            }
        }
        progress();
    }

    let pyodideLoader = require('pyodide.js.js');
    await pyodideLoader.packages(pyDcp[pyPath + 'pyodide.js']);
    progress();

    if (!globalThis.pyScope) globalThis.pyScope = {};
    pyScope = pythonPyodideWheels ? globalThis : {
        setTimeout: globalThis.setTimeout,
    };
    pyScope['dcp'] = {
        progress: progress,
    };

    if (!globalThis.pyLog) globalThis.pyLog = [];
    pyLog = ['// PYTHON LOG START //'];

    if (!globalThis.pyLogger) globalThis.pyLogger = function pyLogger(input)
    {
        globalThis.pyLog.push(input);
    }

    if (!globalThis.pyodide) globalThis.pyodide = await loadPyodide
    (
      {
        indexURL : pyPath,
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
      if ( pythonPackages[i] == 'scipy')
      {
          if ( Object.keys(pyodide.loadedPackages).indexOf('scipy') === -1 ) await pyodide.loadPackage(['CLAPACK']);
      }
      if ( Object.keys(pyodide.loadedPackages).indexOf(pythonPackages[i]) === -1 ) await pyodide.loadPackage([pythonPackages[i]]);

      progress();
    }

    pyodide.globals.set('input_imports', pythonImports);
    pyodide.globals.set('input_modules', pythonModules);

    await pyodide.runPython(pythonInitWorker);

    progress();

    pyodide.globals.set('pickle_function', pythonPickleFunction);
    pyodide.globals.set('pickle_arguments', pythonPickleArguments);
    pyodide.globals.set('pickle_input', pythonPickleInput);
    pyodide.globals.set('pickle_output', pythonPickleOutput);

    pyodide.globals.set('encode_arguments', pythonEncodeArguments);
    pyodide.globals.set('encode_input', pythonEncodeInput);
    pyodide.globals.set('encode_output', pythonEncodeOutput);

    pyodide.globals.set('compress_function', pythonCompressFunction);
    pyodide.globals.set('compress_arguments', pythonCompressArguments);
    pyodide.globals.set('compress_input', pythonCompressInput);
    pyodide.globals.set('compress_output', pythonCompressOutput);

    pyodide.globals.set('colab_pickling', pythonColabPickling);

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

    // TODO: track and verify expected output type, when pickling and encoding are off
    if ( !pythonPickleOutput && pyodide.isPyProxy(sliceOutput) ) sliceOutput = sliceOutput.toJs();

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
    const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);

    let errorObject = {
        error: error,
        index: sliceData['index'],
        elapsed: stopTime,
        stdout: [],
    };

    if (globalThis.pyLog)
    {
        pyLog.push('// PYTHON LOG ERROR //');
        errorObject.stdout = pyLog;
    }

    return errorObject;
  }
}
