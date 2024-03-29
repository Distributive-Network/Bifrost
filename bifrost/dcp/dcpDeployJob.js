exports.pythonWrap = null; // require('fs').readFileSync('../dcp/dcpWorkFunction.js').toString();

exports.pythonInit = null; // require('fs').readFileSync('../dcp/dcp_init_worker.py').toString();

exports.pythonCompute = null; // require('fs').readFileSync('../dcp/dcp_compute_worker.py').toString();

exports.jobParameters = {
  'job_collate': true,
  'job_debug': false,
  'job_greedy': false,
  'job_estimation': 3,
  'job_groups': [],
  'job_public': {
    'name': 'DCP PyJs Deployment',
    'description': false,
    'link': false,
  },
  'job_requirements': {
    'discrete': false
  },
  'job_slice_payment_offer': undefined
};

exports.dcpParameters = {
  'dcp_data': null,
  'dcp_events': {
    'accepted': true,
    'complete': true,
    'console': true,
    'error': true,
    'readystatechange': true,
    'result': true,
  },
  'dcp_debug': false,
  'dcp_kvin': false,
  'dcp_node_js': false,
  'dcp_show_timings': false,
  'dcp_remote_storage_location': false,
  'dcp_remote_storage_params': false,
  'dcp_remote_flags': {
    'input_set': false,
    'work_function': false,
    'work_arguments': false,
    'results': false,
  },
  'dcp_multiplier': 1,
  'dcp_local': 0,
  'dcp_wrapper': exports.pythonWrap,
};

exports.workerParameters = {
  'slice_workload': {
    'workload_function': null,
    'workload_arguments': [],
    'workload_named_arguments': {},
  },
  'python_modules': {},
  'python_imports': [],
  'python_packages': [],
  'python_files': {
    'files_path': [],
    'files_data': {},
  },
  'python_functions': {
    'init': exports.pythonInit,
    'compute': exports.pythonCompute,
  },
};

exports.workerConfigFlags = {
  'pickle': {
    'function': false,
    'arguments': false,
    'input': false,
    'output': false,
  },
  'encode': {
    'function': false,
    'arguments': false,
    'input': false,
    'output': false,
  },
  'compress': {
    'function': false,
    'arguments': false,
    'input': false,
    'output': false,
  },
  'files': {
    'input': false,
  },
  'pyodide': {
    'wheels': true,
  },
  'cloudpickle': false,
};

exports.deploy = async function pyjsDeployJob(
  job_parameters,
  dcp_parameters,
  worker_parameters,
  worker_config_flags,
) {
  async function dcpPost(inputSet, workFunction, sharedArguments, myMultiplier, myLocal) {
    const jobStartTime = Date.now();

    if (job_parameters['job_payment_slice_offer']) {
      job_parameters['job_slice_payment_offer'] = undefined;
    };

    let jobResults = [...Array(inputSet.length / myMultiplier)].map(x => []);

    let jobTimings = [];

    let compute = await require('dcp/compute');

    if (dcp_parameters['dcp_debug']) console.log('DCP Client Build :', await require('dcp/build'));

    let kvin = (dcp_parameters['dcp_kvin']) ? require('kvin') : null;

    // TODO: fully implement the redeployment specifications as articulated below

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

    if (job_parameters['job_estimation'] == -1) {
      job.estimationSlices = Infinity;
    }
    else {
      job.estimationSlices = job_parameters['job_estimation'];
    }

    job.greedyEstimation = job_parameters['job_greedy'];

    job.public = job_parameters['job_public'];

    job.debug = job_parameters['job_debug'];

    job.requirements.discrete = false;

    // set module requirements for python job
    if (dcp_parameters['dcp_node_js'] == false) {
      let versionNamespace = (worker_config_flags['pyodide']['wheels'] == false) ? 'pyodide' : 'pyodide-0.21.0a2';

      let pyodideShards = (worker_config_flags['pyodide']['wheels'] == false) ? null : require('../dcp/pyodide/shards.json');

      function requiresShards(pyFile) {
        if (pyodideShards && typeof pyodideShards['packages'] !== 'undefined' && typeof pyodideShards['packages'][pyFile] !== 'undefined') {
          let fileShards = pyodideShards.packages[pyFile];

          for (let i = 0; i < fileShards.length; i++) {
            let thisFileShard = fileShards[i];
            job.requires(versionNamespace + '-' + thisFileShard.toLowerCase() + '/' + thisFileShard);
          }
          return fileShards.length; // number of file shard packages that are associated with this python file
        }
        else {
          return 0; // file shard packages are not being used, or this package is not on the list
        }
      }

      job.requires(versionNamespace + '-pyodide.asm.data/pyodide.asm.data.js');
      job.requires(versionNamespace + '-pyodide.asm.wasm/pyodide.asm.wasm.js');
      job.requires(versionNamespace + '-pyodide_py.tar/pyodide_py.tar.js');
      job.requires(versionNamespace + '-pyodide.asm.js/pyodide.asm.js.js');
      job.requires(versionNamespace + '-pyodide.js/pyodide.js.js');

      requiresShards('pyodide.asm.wasm');

      if (worker_config_flags['pyodide']['wheels'] == false) {
        job.requires(versionNamespace + '-distutils.data/distutils.data.js');
        job.requires(versionNamespace + '-distutils.js/distutils.js.js');
        job.requires(versionNamespace + '-packages.json/packages.json.js');
      }
      else {
        job.requires(versionNamespace + '-package.json/package.json.js');
        job.requires(versionNamespace + '-distutils.tar/distutils.tar.js');
        job.requires(versionNamespace + '-repodata.json/repodata.json.js');
      }

      let pyodideDepends = (worker_config_flags['pyodide']['wheels'] == false) ? require('../dcp/pyodide/packages.json') : require('../dcp/pyodide/repodata.json');

      let pyodideRequireFiles = pyodideDepends.packages;
      let pyodideRequireFilesKeys = Object.keys(pyodideRequireFiles);
      let pyodideRequireNames = {};
      for (let i = 0; i < pyodideRequireFilesKeys.length; i++) {
        const thatKey = pyodideRequireFilesKeys[i];
        const thisKey = pyodideRequireFiles[thatKey]['name'];
        pyodideRequireNames[thisKey] = thatKey;
      }

      function addToJobRequires(pyFile, requestAncestors = ['distutils']) {
        let packageKey = pyodideRequireNames[pyFile] || pyFile;
        let packageInfo = pyodideRequireFiles[packageKey];
        let packageName = (packageInfo && typeof packageInfo['name'] !== 'undefined') ? packageInfo['name'] : pyFile;
        let packageDepends = (packageInfo && typeof packageInfo['depends'] !== 'undefined') ? packageInfo['depends'] : [];

        requestAncestors.push(packageKey);

        for (dependency of packageDepends) {
          if (!requestAncestors.includes(dependency)) addToJobRequires(dependency, requestAncestors);
        }

        if (worker_config_flags['pyodide']['wheels'] == false) {
          const packageFileDataPath = versionNamespace + '-' + packageName.toLowerCase() + '.data/';
          const packageFileData = packageName + '.data.js';
          job.requires(packageFileDataPath + packageFileData);

          const packageFileJsPath = versionNamespace + '-' + packageName.toLowerCase() + '.js/';
          const packageFileJs = packageName + '.js.js';
          job.requires(packageFileJsPath + packageFileJs);
        }
        else {
          const packageNameFull = (packageInfo && typeof packageInfo['file_name'] !== 'undefined') ? packageInfo['file_name'] : packageName;
          const packageFileJsPath = versionNamespace + '-' + packageNameFull.toLowerCase() + '/';
          const packageFileJs = packageNameFull + '.js';
          job.requires(packageFileJsPath + packageFileJs);
          requiresShards(packageNameFull); // check if this file is broken into a package for each shard, and add to job.requires accordingly
        }
      }

      if (worker_config_flags['pyodide']['wheels'] == false && worker_config_flags['cloudpickle'] == true) worker_parameters['python_packages'].push('cloudpickle');

      for (let i = 0; i < worker_parameters['python_packages'].length; i++) {
        let thisPackageName = worker_parameters['python_packages'][i];
        addToJobRequires(thisPackageName);
      }
    }
    else {
      for (let i = 0; i < worker_parameters['python_packages'].length; i++) {
        let thisPackageName = worker_parameters['python_packages'][i];
        job.requires(thisPackageName);
      }
    }
    job.requires('aitf-compress/pako');

    job.requires('bifrost-kvin/bf-kvin.js');

    let eventFunctions = {
      accepted: () => { },
      complete: () => { },
      console: () => { },
      error: () => { },
      readystatechange: () => { },
      result: () => { },
    };

    let jobResultInterval;
    let resultIntervalFunction = function() {
      job.results.fetch(null, emitEvents = true); // TODO : configurable flags
      const fetchResultCount = Array.from(job.results).length;
      if (job.debug) console.log('Job Result Fetch Count', ':', fetchResultCount, ':', Date.now());
      // TODO : support for (myMultiplier > 1)
      if (!dcp_parameters['dcp_kvin'] && fetchResultCount >= inputSet.length) resolve({ bifrostResultHandle: job.results });
    }

    async function dcpPromise() {
      return new Promise(function(resolve, reject) {
        eventFunctions.accepted = function onJobAccepted() {
          console.log('Accepted :', job.id);

          jobId = job.id;

          // TODO : make contingent on certain conditions or flags
          // TODO : configurable result threshold for resolving
          // TODO : configurable timer value, flag for interval vs single-shot timeout
          jobResultInterval = setInterval(resultIntervalFunction, 60000);
        }

        eventFunctions.complete = function onJobComplete(myComplete) {
          console.log('Complete :', job.id);

          const completeResultCount = Array.from(myComplete).length;
          // TODO : support for (myMultiplier > 1)
          if (completeResultCount >= inputSet.length) resolve({ bifrostResultHandle: myComplete });
        }

        eventFunctions.console = function onJobConsole(myConsole) {
          console.log(myConsole.sliceNumber + ' : ' + myConsole.level, ' : ' + myConsole.message);
        }

        eventFunctions.error = function onJobError(myError) {
          console.log(myError.sliceNumber + ' : error : ' + myError.message);
        }

        eventFunctions.readystatechange = function onJobReadyStateChange(myReadyStateChange) {
          console.log('State :', myReadyStateChange);
        }

        eventFunctions.result = function onJobResult(myResult) {
          let kvinMimeString = 'data:application/x-kvin,';

          if (dcp_parameters['dcp_kvin'] && typeof myResult.result == 'string' && myResult.result.includes(kvinMimeString)) myResult.result = kvin.deserialize(myResult.result.slice(kvinMimeString.length));

          if (myResult.result.hasOwnProperty('output')) {
            if (jobResults[myResult.result.index].length == 0) {
              jobResults[myResult.result.index] = myResult.result.output;

              jobTimings.push(parseInt(myResult.result.elapsed, 10));

              let percentComputed = ((jobTimings.length / jobResults.length) * 100).toFixed(2);

              console.log('Computed : ' + percentComputed + '%');

              if ((dcp_parameters['dcp_node_js'] == false) && job.debug) console.log(myResult.result.index, ': Python Log :', myResult.result.stdout);
            }

            let emptyIndexArray = jobResults.filter(thisResult => thisResult.length == 0);

            if (myMultiplier > 1) console.log('Unique Slices Remaining : ' + emptyIndexArray.length);

            if (emptyIndexArray.length == 0) {
              resolve(jobResults);
            }
          }
          else if (myResult.result.hasOwnProperty('error')) {
            console.log(myResult.result.index, ': Slice Error :', myResult.result.error);
            if (dcp_parameters['dcp_node_js'] == false) console.log(myResult.result.index, ': Python Log :', myResult.result.stdout);
          }
          else {
            console.log('Bad Result (no "output" property) :', myResult);
          }

          clearInterval(jobResultInterval);
          jobResultInterval = setInterval(resultIntervalFunction, 60000);
        }

        for (event in dcp_parameters['dcp_events']) {
          if (dcp_parameters['dcp_events'][event] && eventFunctions[event]) job.on(event, eventFunctions[event]);
        }

        let execResults;

        if (myLocal > 0) {
          execResults = job.localExec(myLocal);
        }
        else {
          if (job_parameters['job_slice_payment_offer']) {
            execResults = job.exec(job_parameters['job_slice_payment_offer']);
          } else {
            execResults = job.exec();
          }
        }

        function execHandler(promiseExec) {
          const execResultCount = Array.from(promiseExec).length;
          // TODO : support for (myMultiplier > 1)
          if (execResultCount >= inputSet.length) resolve({ bifrostResultHandle: promiseExec });
        }

        execResults.then(execHandler);
      });
    }

    let dcpPromiseResults = await dcpPromise();

    clearInterval(jobResultInterval);

    for (event in dcp_parameters['dcp_events']) {
      if (dcp_parameters['dcp_events'][event] && eventFunctions[event]) job.removeEventListener(event, eventFunctions[event]);
    }

    if (dcpPromiseResults['bifrostResultHandle']) {
      let handleResults = Array.from(dcpPromiseResults['bifrostResultHandle']);
      for (let i = 0; i < handleResults.length; i++) {
        let myResult = handleResults[i];

        if (myResult.hasOwnProperty('output')) {
          if (jobResults[myResult.index].length == 0) {
            jobResults[myResult.index] = myResult.output;

            jobTimings.push(parseInt(myResult.elapsed, 10));
          }
        }
        else if (myResult.hasOwnProperty('error')) {
          console.log(myResult.index, ': Slice Error :', myResult.error);
          if (dcp_parameters['dcp_node_js'] == false) console.log(myResult.index, ': Python Log :', myResult.stdout);
        }
        else {
          console.log('Bad Result (no "output" property) : ' + myResult);
        }
      }
    }

    if (!jobId) jobId = job.id;

    // this the end of the redeployment zone

    // nothing after this point should ever be called more than once as part of the same user-submitted job.
    // time metrics especially must account for all redeployment attempts, and can never reset in between.

    if (dcp_parameters['dcp_show_timings']) {
      const averageSliceTime = jobTimings.reduce((a, b) => a + b) / jobResults.length;
      const totalJobTime = Date.now() - jobStartTime;

      console.log('Total Elapsed Job Time: ' + (totalJobTime / 1000).toFixed(2) + ' s');
      console.log('Mean Elapsed Worker Time Per Slice: ' + averageSliceTime + ' s');
      console.log('Mean Elapsed Client Time Per Unique Slice: ' + ((totalJobTime / 1000) / jobResults.length).toFixed(2) + ' s');
    }

    return jobResults;
  }

  async function addKeystore(keystoreInput, ksPassword) {
    let dcpWallet = await require('dcp/wallet');

    let keystoreObject = (typeof keystoreInput == 'string') ? JSON.parse(keystoreInput) : keystoreInput;
    let bankKeystore = await (new dcpWallet.Keystore(keystoreObject));
    let idKeystore = await (new dcpWallet.IdKeystore(null, ""));

    if (ksPassword) {
      if (dcp_parameters['dcp_debug']) console.log("Keystore password was provided. Unlocking wallet.");
      await bankKeystore.unlock(ksPassword, 24 * 60 * 60 * 1000);
      dcpWallet.passphrasePrompt = (message) => {
        return ksPassword;
      };
    }

    await dcpWallet.add(bankKeystore);
    await dcpWallet.addId(idKeystore);
  }

  if (job_parameters['job_payment_account'] !== false) {
    await addKeystore(job_parameters['job_payment_account'], job_parameters['job_payment_account_password']);
  }

  let jobData = dcp_parameters['dcp_data'];
  let jobMultiplier = dcp_parameters['dcp_multiplier'];
  let jobLocal = dcp_parameters['dcp_local'];

  let workFunction;
  let sharedArguments;

  if (dcp_parameters['dcp_node_js'] == false) {
    workFunction = dcp_parameters['dcp_wrapper'];

    sharedArguments = [
      worker_parameters,
      worker_config_flags,
    ];
  }
  else {
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
    sharedArguments = [jobArguments];
  }

  let inputSet = [];
  jobData.forEach(x => {
    let myItem = Object.fromEntries(Object.entries(x));
    inputSet.push(myItem);
    return [];
  });

  jobId = null;

  try {
    jobOutput = await dcpPost(inputSet, workFunction, sharedArguments, jobMultiplier, jobLocal);
  }
  catch (error) {
    console.log('Deploy Job Error :', error);
  }
};
