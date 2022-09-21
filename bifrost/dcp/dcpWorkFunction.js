async function workFunction(
    sliceData,// input slice, primary arg to user-provided function
    workerParameters,// dictionary of supplemental args, libraries, functions, and files
    workerConfigFlags,// dictionary of boolean flags prescribing various encoding-related behaviours
)
{
  const startTime = Date.now();

  try
  {
    progress();

    function parameterValidation(inputParameter)
    {
      if (typeof inputParameter == 'string') inputParameter = JSON.parse(inputParameter);
      if (inputParameter["_serializeVerId"])
      {
        inputParameter = require('kvin').unmarshal(inputParameter);
      }
      return inputParameter;
    }

    sliceData = parameterValidation(sliceData);
    workerParameters = parameterValidation(workerParameters);
    workerConfigFlags = parameterValidation(workerConfigFlags);

    if (!globalThis.pyDcp) globalThis.pyDcp = {};

    if (typeof location !== 'undefined')
    {
      if (workerConfigFlags['pyodide']['wheels'])
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
        if (pyDcp[input] + pyDcp[input].buffer)
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

    async function loadBinaryWrap(indexURL, path, subResourceHash)
    {
        let response = await globalThis.fetch(path);

        if (!response.ok) throw new Error(`Failed to load '${path}': request failed.`);

        return new Uint8Array(await response.arrayBuffer());
    }
    globalThis.loadBinaryWrap = loadBinaryWrap;

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

    if (workerConfigFlags['pyodide']['wheels']) globalThis.URL = function(...args){ return args[0] };

    globalThis.WebAssembly.instantiateStreaming = null;

    function frankenDoctor
    (
      frankenBody,
      frankenHead,
      frankenBrain = '',
    )
    {
      let frankenNeck = frankenBody.indexOf(frankenHead);

      let frankenMonster = (frankenNeck > -1) ? frankenBody.slice(0, frankenNeck) + frankenBrain + frankenBody.slice(frankenNeck + frankenHead.length) : frankenBody;

      return frankenMonster;
    }

    async function requirePyFile(fileName)
    {
        let fileLoader = require(fileName + '.js');
        await fileLoader.download();
        let fileDecode = await fileLoader.decode();

        if (fileName == 'pyodide.asm.js' || fileName == 'pyodide.js')
        {
          let frankenFile = await frankenDoctor
          (
            fileDecode,
            'loadBinaryFile(',
            'loadBinaryWrap(',
          );
          fileDecode = frankenFile;
        }

        // source maps are referenced in the last line of some js files; we want to strip these urls out, as the source maps will not be available
        if (fileLoader.PACKAGE_FORMAT == 'string' && fileName.includes('.js') && typeof fileDecode == 'string')
        {
            let sourceMappingIndex = fileDecode.indexOf('//' + '#' + ' ' + 'sourceMappingURL'); // break up the string to avoid a potential resonance cascade
            if (sourceMappingIndex != -1) fileDecode = fileDecode.slice(0, sourceMappingIndex);
        }

        return fileDecode;
    }

    let pyPath = workerConfigFlags['pyodide']['wheels'] ? '/' : './';

    let pyFiles =
    [
      { filepath: pyPath, filename: 'pyodide.asm.data'},
      { filepath: pyPath, filename: 'pyodide.asm.wasm'},
      { filepath: pyPath, filename: 'pyodide_py.tar'},
      { filepath: pyPath, filename: 'pyodide.asm.js'},
      { filepath: pyPath, filename: 'pyodide.js'},
    ];

    if (workerConfigFlags['pyodide']['wheels'])
    {
        pyFiles.push({ filepath: pyPath, filename: 'package.json'});
        pyFiles.push({ filepath: pyPath, filename: 'distutils.tar'});
    }
    else
    {
        pyFiles.push({ filepath: pyPath, filename: 'distutils.data'});
        pyFiles.push({ filepath: pyPath, filename: 'distutils.js'});
    }

    let jsonFileName = workerConfigFlags['pyodide']['wheels'] ? 'repodata.json' : 'packages.json';

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

        if (!workerConfigFlags['pyodide']['wheels'])
        {
            const packageFileData = packageName + '.data';
            pyFiles.push({ filepath: pyPath, filename: packageFileData });
        }

        const packageFileJs = ( packageInfo && typeof packageInfo['file_name'] !== 'undefined' ) ? packageInfo['file_name'] : packageName + '.js';
        pyFiles.push({ filepath: pyPath, filename: packageFileJs });
    }

    if ( workerConfigFlags['pyodide']['wheels'] == false && workerConfigFlags['cloudpickle'] == true ) workerParameters['python_packages'].push('cloudpickle');

    for (let i = 0; i < workerParameters['python_packages'].length; i++)
    {
      const packageName = workerParameters['python_packages'][i];

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
    pyScope = workerConfigFlags['pyodide']['wheels'] ? globalThis : {
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

    for (let i = 0; i < workerParameters['python_packages'].length; i++)
    {
        await loadPyPackage(workerParameters['python_packages'][i]); // XXX AWAIT PROMISE ARRAY, IF ORDER CAN BE RESOLVED?

        progress();
    }

    pyodide.globals.set('input_imports', workerParameters['python_imports']);
    pyodide.globals.set('input_modules', workerParameters['python_modules']);

    if (workerConfigFlags['files']['input'])
    {
        workerParameters['python_files']['files_path'].push(sliceData['path']);
        workerParameters['python_files']['files_data'][sliceData['path']] = sliceData['binary'];
    }

    pyodide.globals.set('input_files_path', workerParameters['python_files']['files_path']);
    pyodide.globals.set('input_files_data', workerParameters['python_files']['files_data']);

    await pyodide.runPython(workerParameters['python_functions']['init']);

    /*
    progress(1);

    pyLog.push('// PYTHON LOG STOP //');

    const stopTime = ((Date.now() - startTime) / 1000).toFixed(2);

    return {
        output: 0,
        index: sliceData['index'],
        elapsed: stopTime,
        stdout: pyLog,
    };
    */

    progress();

    pyodide.globals.set('worker_config_flags', workerConfigFlags);

    if (workerConfigFlags['pickle']['function'])
    {
        pyodide.globals.set('input_function', workerParameters['slice_workload']['workload_function']);
    }
    else
    {
        await pyodide.runPython(workerParameters['slice_workload']['workload_function']['code']);
        pyodide.globals.set('input_function', workerParameters['slice_workload']['workload_function']['name']);
    }

    pyodide.globals.set('input_data', sliceData['data']);
    pyodide.globals.set('input_parameters', workerParameters['slice_workload']['workload_arguments']);
    pyodide.globals.set('input_keyword_parameters', workerParameters['slice_workload']['workload_named_arguments']);

    await pyodide.runPython(workerParameters['python_functions']['compute']);

    progress(1);

    pyLog.push('// PYTHON LOG STOP //');

    let sliceOutput = pyodide.globals.get('output_data');

    // TODO: track and verify expected output type, when pickling and encoding are off
    if ( !workerConfigFlags['pickle']['output'] && pyodide.isPyProxy(sliceOutput) ) sliceOutput = sliceOutput.toJs();

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
