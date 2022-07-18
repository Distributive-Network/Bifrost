// MODULES

// node built-in modules
const crypto    = require('crypto');
const stream    = require('stream');
const vm        = require('vm');

// npm modules
const mmap      = require(process.cwd() + '/node_modules/' + '@raygun-nickj/mmap-io');
const xxhash    = require(process.cwd() + '/node_modules/' + 'xxhash-wasm');

// local modules
const deepEqual = require('./deepEqual.js').deepEqual;
const npy       = require('./npy-js');
const utils     = require('./utils');

// PROGRAM

// set up arguments
const args      = process.argv;

const SHM_FILE_NAME = args[args.length-1];
const BIFROST_WINDOWS = args[args.length-2];
const BIFROST_NOTEBOOK = args[args.length-3];
const BIFROST_MP_SHARED = args[args.length-4];
const BIFROST_SHARED = args[args.length-5];

// we only pipe the errors through stdout if we are in a NON-WINDOWS AND NON-NOTEBOOK environment
if ( BIFROST_NOTEBOOK == "False" && BIFROST_WINDOWS == "False" ) process.stderr.pipe(process.stdout);
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

        this.bytesPending = 0;
        this.fragmentList = [];

        this.cache = {};
        this.fd = -1;
        let size= Math.floor( 0.75 * 1024*1024*1024 );

        if ( BIFROST_SHARED == "fs")
        {
            const fs = require('fs');

            this.fd = fs.openSync
            (
                SHM_FILE_NAME,
                'r+',
            );
        }
        else
        {
            const shm = require(process.cwd() + '/node_modules/' + 'shmmap');

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
            if (!!obj && obj['type'] == 'infinity'){
                obj = Infinity;
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

    chunk = chunk.toString('utf-8');

    if (evaluator.fragmentList.length == 0)
    {
        // Each message begins with header from E00000000C to EffffffffC
        // : E in position 0, indicating extended message
        // : C in position 9, indicating concatenated message
        // : hexademical digits from 0 to f in positions 1 to 8, together indicating message total length
        // : message length in header includes Bifrost's JSON wrapping, but does not include header itself

        const header = chunk.slice(0,10);

        if (header[0] !== "E" || header[9] !== "C") throw("bad message header");

        const scriptLength = parseInt(header.slice(1, 9), 16);

        evaluator.bytesPending = scriptLength;

        chunk = chunk.slice(10);
    }

    evaluator.fragmentList.push(chunk);
    evaluator.bytesPending = evaluator.bytesPending - chunk.length;

    if (evaluator.bytesPending == 0)
    {
        let scriptString = evaluator.fragmentList.join("");
        evaluator.fragmentList = [];

        evaluator.syncFrom();
        try{
            let scriptJSON = JSON.parse(scriptString);
            let script = scriptJSON['script'];
            await evaluator.evaluate(script);

        }catch(err){
            console.log("Error occured during script running/parsing: ", err);
        }
        evaluator.syncTo();
        console.log('{"type": "done"}')
    }
    else if (evaluator.bytesPending < 0)
    {
        throw("negative pending bytes", evaluator.bytesPending);
    }
    done();
}


process.stdin.pipe(inputStream);
