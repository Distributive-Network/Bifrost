const stream    = require('stream');
const vm        = require('vm');
const utils     = require('./utils');
const mmap      = require('@raygun-nickj/mmap-io');
const npy       = require('./npy-js');
const xxhash    = require('xxhash-wasm');
const crypto    = require('crypto');
const args      = process.argv;
const deepEqual = require('./deepEqual.js').deepEqual;
const SHM_FILE_NAME = args[args.length-1];
const BIFROST_WINDOWS = args[args.length-2];
const BIFROST_NOTEBOOK = args[args.length-3];
const BIFROST_MP_SHARED = args[args.length-4];

const fs = require('fs');

console.log("Beginning Node Process");

// we only pipe the errors through stdout if we are in a NON-WINDOWS AND NON-NOTEBOOK environment
if ( BIFROST_NOTEBOOK == false && BIFROST_WINDOWS == false ) process.stderr.pipe(process.stdout);
/**
 * Evaluator class is the main class meant to evaluate any node script given
 * using some node context.
 * 
 * It also holds references to the mmap'ed buffer
 */
class Evaluator{
    constructor(){
        this.context                     = global;
        this.context['parseNumpyFile']   = npy.parseNumpyFile;
        this.context['unparseNumpyFile'] = npy.unparseNumpyFile;
        this.context['buildDataArray']   = npy.buildDataArray;
        this.context['require']          = require;
        vm.createContext(this.context);
        console.log("VM context has been prepared.");
        this.cache = {};
        this.fd = -1;
        let size= Math.floor( 0.75 * 1024*1024*1024 );

        if ( BIFROST_WINDOWS == true || BIFROST_MP_SHARED == false )
        {
            this.fd = fs.openSync
            (
                SHM_FILE_NAME,
                'r+',
            );
        }
        else
        {
            const shm = require('shmmap');

            this.fd = shm.open
            (
                SHM_FILE_NAME,
                shm.O_RDWR,
                600,
            );
        }

        this.mm= mmap.map
        (
            size,
            mmap.PROT_READ | mmap.PROT_WRITE,
            mmap.MAP_SHARED,
            this.fd,
            0,
        );

        this.dontSync = Object.keys(this.context);
        this.seed = crypto.randomBytes(8); 
    }

    /**
     * Check if some variable is in cache
     * 
     * @param {string} key 
     * @param {any} val 
     * @returns 
     */
    inCache( key, val){
      let results = { 'bool': false, 'hash': '' }; 
      results.hash = this.hash64( val, this.seed );

      if (typeof this.cache[key] !== 'undefined'){
        if (this.cache[key] === results.hash){
          results.bool = true;
        }
      }
      return results;
    };

    /**
     * Set some hash in the cache.
     * 
     * @param {string} key 
     * @param {string} hsh 
     */
    setCache( key, hsh){
      this.cache[key] = hsh;
    };

    /**
     * Sync to the python process from the node process
     * Using the mmap buffer.
     */
    syncTo(){
        let final_output = {};
        let allVarsToSync = Object.keys(this.context).filter(x=> !this.dontSync.includes(x)); //all variables that are not in the default context;
        allVarsToSync = new Set(allVarsToSync);
        this.toSync.forEach(item=> allVarsToSync.add(item));
        allVarsToSync = Array.from(allVarsToSync);

        for (let key of allVarsToSync){
            try{
                //if it is a dataArray, convert back to numpy!
                if (!!this.context[key] && typeof this.context[key] !== 'undefined' && typeof this.context[key].constructor !== 'undefined' && this.context[key].constructor.name === 'DataArray'){
                        let toCheck = Buffer.concat([
                          Buffer.from(this.context[key].typedArray.buffer),
                          Buffer.from(this.context[key].shape)
                        ]);


                        let cacheResults = this.inCache( key, toCheck );
                        if (cacheResults.bool){
                          continue;
                        }else{
                          this.setCache( key, cacheResults.hash );
                          let abData = npy.unparseNumpyFile(this.context[key]).buffer;
                          let data = Buffer.from(abData, 'binary').toString('base64');
                          final_output[key] = {
                              'type': 'numpy',
                              'data': data
                          };
                        };
                    }else{
                        if (this.context[key] === undefined){
                          throw 'undefined value error'
                        }
                        let val = JSON.stringify(this.context[key]);
                        if (deepEqual(JSON.parse(val), this.context[key])){
                          let cacheResults = this.inCache( key, Buffer.from(val) );
                          if (cacheResults.bool){
                            continue;
                          }else{
                            this.setCache( key, cacheResults.hash );
                            final_output[key] = this.context[key]; 
                          }
                        }
                  }
            }catch(err){
                // console.log(err);
                if (err == 'undefined value error'){
                  throw 'UNDEFINED VALUE ERROR: JavaScript variable to be synced contains an undefined value. Undefined values are not supported by Python.'
                }
                continue;
            }
        }
        let newVars = Object.keys(this.context).filter(x=> !this.dontSync.includes(x));
        newVars = newVars.filter

        this.toSync = [];

        let final_str = JSON.stringify(final_output);
        //make sure it is utf-8
        final_str = Buffer.from(final_str, 'utf-8').toString('utf-8');

        this.mm.write(final_str + '\n');

    }



    /**
     * Sync from the python process into the node process.
     */
    syncFrom(){
        var buffToParse = this.mm.slice(0, this.mm.indexOf('\n')); 
        var jsonVars    = JSON.parse(buffToParse.toString('utf-8'));
        for (let key of Object.keys(jsonVars)){
            let obj = jsonVars[key];
            if (!!obj && obj['type'] == 'numpy' && typeof obj['data'] === 'string'){
                let data = obj['data'];
                let abData = utils.strtoab(data);
                const npArr = npy.parseNumpyFile(abData);
                obj = npArr;
            }
            this.context[key] = obj;
        }
        this.toSync = Object.keys(jsonVars);
    }

    //evaluate some script in the given context.
    //Note that we await in order to allow for scripts which end in a promise
    //to fully complete
    async evaluate(script){
        await vm.runInContext(script, this.context);
        //console.log("Done evaluating");
    }
}

const evaluator = new Evaluator();

//Modify the input stream.
let inputStream = new stream.Transform();
/**
 * Modify the input stream so that when this process
 * Get's something on stdin, we do the following:
 * 1. sync from the python process.
 * 2. Parse the script
 * 3. Evaluate the script
 * 4. Sync back to the python process.
 * 5. Tell the python process we are done.
 */
inputStream._transform = async function(chunk, encoding, done){

    if (!evaluator.hash64)
    {
        let { h64 } = await xxhash();
        evaluator.hash64 = h64;
    }

    evaluator.syncFrom();
    try{
        let scriptJSON = JSON.parse(chunk.toString('utf-8'));
        let script = scriptJSON['script'];
        await evaluator.evaluate(script);

    }catch(err){
        console.log("Error occured during script running/parsing: ", err);
    }
    evaluator.syncTo();
    console.log('{"type": "done"}')
    this.push(chunk);
    done();
}


process.stdin.pipe(inputStream);
