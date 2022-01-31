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

        // TODO: consolidate calls to pyDcp file cache into a single function
        class PyodideXMLHttpRequest
        {
            method;
            async;
            url;
            body;

            //response;
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
                  throw('Missing file:', this.url, '(xhr.open)');
                }
                return 1;
            }

            send(body = null)
            {
                return 1;
            }
        }
        globalThis.XMLHttpRequest = PyodideXMLHttpRequest;

        // TODO: this is a meme function, DO NOT USE outside of development and diagnostics!
        let crypto =
        {
            getRandomValues: function(typedArray)
            {
                return typedArray.map(x => Math.floor(Math.random() * 256)); // currently assumes Uint8; TODO: populate based on array type checking
            }
        };
        globalThis.crypto = crypto;

        // TODO: restrict file retrieval to web worker behaviours only
        let process = {};
        globalThis.process = process;

         // TODO: make an empty fs polyfill available by default, or avoid needing to do this
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

        // TODO: investigate pyodide's existing python fetch integrate
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

        // TODO: more seamless invocation of BravoJS require
        async function importScripts(...args)
        {
            for (let i = 0; i < args.length; i++)
            {
                let thisArg = args[i];

                if (pyDcp[thisArg])
                {
                    //await eval(pyDcp[thisArg]);

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

        // TODO: are these necessary?
        globalThis.AbortController = function(...args)
        {
            return {};
        };
        globalThis.FormData = function(...args)
        {
            return {};
        };
        globalThis.URLSearchParams = function(...args)
        {
            return {};
        };

        // TODO: these are the dry bones of ancient stripped-down infrastructure; at this point, it just turns off instantiateStreaming
        function wasmWedge( wedgeStreaming = false )
        {
          const wasmInstantiateStreaming = WebAssembly.instantiateStreaming;
          const wasmInstantiate = WebAssembly.instantiate;

          async function wasmStreamingDestream(binary, info)
          {
              const binaryInput = await binary.arrayBuffer();
              const result = await wasmInstantiate(binaryInput, info);
              return result;
          }

          async function wasmDestream(binary, info)
          {
              const result = await wasmInstantiate(binary, info);
              return result;
          }

          if (wedgeStreaming)
          {
            globalThis.WebAssembly.instantiateStreaming = wasmDestream;
          }
          else
          {
            globalThis.WebAssembly.instantiateStreaming = null;
          }

          globalThis.WebAssembly.instantiate = wasmDestream;
        }
        wasmWedge();

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
