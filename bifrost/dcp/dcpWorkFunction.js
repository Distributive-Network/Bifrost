async function workFunction(
    sliceData,// input slice, primary arg to user-provided function
    sliceParameters,// shared positional parameters, secondary args to user-provided function
    sliceNamedParameters,// shared keyword parameters, tertiary args to user-provided function
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

    if (!globalThis.pyDcp) globalThis.pyDcp = {};

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
              throw('Missing file: ' + input + ' (xhr.open)');
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
          throw('Missing file: ' + input + ' (fetch.arrayBuffer)');
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
          throw('Missing file: ' + input + ' (fetch.json)');
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
                throw('Missing file: ' + thisArg + ' (importScripts)');
            }
        }
    }
    globalThis.importScripts = importScripts;

    if (pythonPyodideWheels) globalThis.URL = function(...args){ return args[0] };

    globalThis.WebAssembly.instantiateStreaming = null;

    function frankenDoctor
    (
      frankenBody,
      frankenHead,
      frankenBrain = '',
    )
    {
      let frankenNeck = frankenBody.indexOf(frankenHead);

      let frankenMonster = frankenBody.slice(0, frankenNeck) + frankenBrain + frankenBody.slice(frankenNeck + frankenHead.length);

      return frankenMonster;
    }

    async function requirePyFile(fileName)
    {
        let fileLoader = require(fileName + '.js');
        await fileLoader.download();
        let fileDecode = await fileLoader.decode();

        // source maps are referenced in the last line of some js files; we want to strip these urls out, as the source maps will not be available
        if (fileLoader.PACKAGE_FORMAT == 'string' && fileName.includes('.js') && typeof fileDecode == 'string')
        {
            let sourceMappingIndex = fileDecode.indexOf('//' + '#' + ' ' + 'sourceMappingURL'); // break up the string to avoid a potential resonance cascade
            if (sourceMappingIndex != -1) fileDecode = fileDecode.slice(0, sourceMappingIndex);
        }

        return fileDecode;
    }

    let pyPath = pythonPyodideWheels ? '/' : './';

    let pyFiles =
    [
      { filepath: pyPath, filename: 'pyodide.asm.data'},
      { filepath: pyPath, filename: 'pyodide.asm.wasm'},
      { filepath: pyPath, filename: 'pyodide_py.tar'},
      { filepath: pyPath, filename: 'pyodide.asm.js'},
      { filepath: pyPath, filename: 'pyodide.js'},
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

    let jsonFileName = pythonPyodideWheels ? 'repodata.json' : 'packages.json';

    let jsonFileKey = pyPath + jsonFileName;
    if (!pyDcp[jsonFileKey])
    {
        pyDcp[jsonFileKey] = await requirePyFile(jsonFileName);
    }

    let pyodideRequireFiles = JSON.parse(pyDcp[jsonFileKey])['packages'];
    let pyodideRequireFilesKeys = Object.keys(pyodideRequireFiles);
    let pyodideRequireNames = {};
    for (let i = 0; i < pyodideRequireFilesKeys.length; i++)
    {
        const thatKey = pyodideRequireFilesKeys[i];
        const thisKey = pyodideRequireFiles[thatKey]['name'];
        pyodideRequireNames[thisKey] = thatKey;
    }

    globalThis.pyodideRequireFiles = pyodideRequireFiles;
    globalThis.pyodideRequireNames = pyodideRequireNames;

    function addToPyFiles(pyFile, requestAncestors = [])
    {
        let packageKey = globalThis.pyodideRequireNames[pyFile] || pyFile;
        let packageInfo = globalThis.pyodideRequireFiles[packageKey];
        let packageName = ( packageInfo && typeof packageInfo['name'] !== 'undefined' ) ? packageInfo['name'] : pyFile;
        let packageDepends = ( packageInfo && typeof packageInfo['depends'] !== 'undefined' ) ? packageInfo['depends'] : [];

        requestAncestors.push(packageKey);

        for (dependency of packageDepends)
        {
            if (!requestAncestors.includes(dependency)) addToPyFiles(dependency, requestAncestors);
        }

        if (!pythonPyodideWheels)
        {
            const packageFileData = packageName + '.data';
            pyFiles.push({ filepath: pyPath, filename: packageFileData });
        }

        const packageNameFull = ( packageInfo && typeof packageInfo['file_name'] !== 'undefined' ) ? packageInfo['file_name'] : packageName;
        const packageFileJs = packageNameFull + '.js';
        pyFiles.push({ filepath: pyPath, filename: packageFileJs });
    }

    if (!pythonPyodideWheels) pythonPackages.push('cloudpickle');

    for (let i = 0; i < pythonPackages.length; i++)
    {
      const packageName = pythonPackages[i];

      addToPyFiles(packageName);
    }

    for (let i = 0; i < pyFiles.length; i++)
    {
        let fileKey = pyFiles[i].filepath + pyFiles[i].filename;
        if (!pyDcp[fileKey])
        {
            pyDcp[fileKey] = await requirePyFile(pyFiles[i].filename);

            // supplemental keys for other potential pyodide loading paths
            pyDcp[pyFiles[i].filename] = pyDcp[fileKey];
            pyDcp['//' + pyFiles[i].filename] = pyDcp[fileKey];
            if (typeof location !== 'undefined')
            {
                if (typeof location['href'] !== 'undefined') pyDcp[location.href + '/' + pyFiles[i].filename] = pyDcp[fileKey];
                if (typeof location['pathname'] !== 'undefined') pyDcp[location.pathname + '/' + pyFiles[i].filename] = pyDcp[fileKey];
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

    async function loadPyPackage(newPackageKey)
    {
        let packageKey = globalThis.pyodideRequireNames[newPackageKey] || newPackageKey;
        let packageInfo = globalThis.pyodideRequireFiles[packageKey];
        let packageName = ( packageInfo && typeof packageInfo['name'] !== 'undefined' ) ? packageInfo['name'] : newPackageKey;

        let loadedKeys = Object.keys(pyodide.loadedPackages);

        if ( loadedKeys.indexOf(packageName) === -1 ) await pyodide.loadPackage([packageName]);
    }

    for (let i = 0; i < pythonPackages.length; i++)
    {
        await loadPyPackage(pythonPackages[i]); // XXX AWAIT PROMISE ARRAY, IF ORDER CAN BE RESOLVED?

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
    pyodide.globals.set('input_keyword_parameters', sliceNamedParameters);

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
