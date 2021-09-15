const stream    = require('stream');
const vm        = require('vm');
const utils     = require('./utils');
//const shm       = require('../build/Release/shmjs.node');
const shm       = require('shmmap');
const mmap      = require('mmap.js');
const npy       = require('./npy-js');
const XXHash    = require('xxhash');
const crypto    = require('crypto');
const args      = process.argv;
const deepEqual = require('./deepEqual.js').deepEqual;
const SHM_FILE_NAME = args[args.length-1];


// Begin by piping stderr into stdout
process.stderr.pipe(process.stdout);



console.log("Beginning Node Process");

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

        //let fd = shm.open(SHM_FILE_NAME, shm.O_RDWR, 600);
        let fd = shm.open(SHM_FILE_NAME);

        this.mm= mmap.alloc(size, mmap.PROT_READ | mmap.PROT_WRITE,
            mmap.MAP_SHARED, fd, 0);

        //this.mm= shm.read_write(SHM_FILE_NAME, size);
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
      results.hash = XXHash.hash64( val , this.seed);

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
                if (typeof this.context[key] !== 'undefined'){
                    //if it is a dataArray, convert back to numpy!
                    if (typeof this.context[key].constructor !== 'undefined' && this.context[key].constructor.name === 'DataArray'){
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
                }
            }catch(err){
                // console.log(err);
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
            if (obj['type'] == 'numpy' && typeof obj['data'] === 'string'){
                let data = obj['data'];
                let abData = Buffer.from(data, 'base64').buffer;
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
