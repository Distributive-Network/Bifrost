const stream = require('stream');
const vm     = require('vm');
const utils  = require('./utils');
const shm    = require('nodeshm');
const mmap   = require('mmap.js');
const npy    = require('npy-js');
const args   = process.argv;
const SHM_FILE_NAME = args[args.length-1];



process.stderr.pipe(process.stdout);



console.log("Beginning Node Process");

class Evaluator{
    constructor(){
        this.context = global;
        this.context['parseNumpyFile']   = npy.parseNumpyFile;
        this.context['unparseNumpyFile'] = npy.unparseNumpyFile;
        this.context['buildDataArray']   = npy.buildDataArray;
        vm.createContext(this.context);
        console.log("VM context has been prepared.");

        this.fd = -1;
        let size= Math.floor( 0.75 * 1024*1024*1024 );

        let fd = shm.shm_open(SHM_FILE_NAME, shm.O_RDWR, 600);

        this.mm= mmap.alloc(size, mmap.PROT_READ | mmap.PROT_WRITE,
            mmap.MAP_SHARED, fd, 0);
        this.dontSync = Object.keys(this.context);
    }

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
                        let abData = npy.unparseNumpyFile(this.context[key]).buffer;
                        let data = utils.abtostr(abData);

                        final_output[key] = {
                            'type': 'numpy',
                            'data': data
                        };
                    }else{
                        let val = JSON.stringify(this.context[key]);
                        final_output[key] = val; 
                    }
                }
            }catch(err){
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




    syncFrom(){
        var buffToParse = this.mm.slice(0, this.mm.indexOf('\n')); 
        var jsonVars    = JSON.parse(buffToParse.toString('utf-8'));
        for (let key of Object.keys(jsonVars)){
            let obj = jsonVars[key];
            if (obj['type'] == 'numpy' && typeof obj['data'] === 'string'){
                let data = obj['data'];
                let abData = utils.strtoab(data);
                const npArr = npy.parseNumpyFile(abData);
                obj = npArr;
            }
            this.context[key] = obj;
        }
        this.toSync = Object.keys(jsonVars);
    }

    async evaluate(script){
        await vm.runInContext(script, this.context);
        //console.log("Done evaluating");
    }
}

const evaluator = new Evaluator();


let inputStream = new stream.Transform();
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
