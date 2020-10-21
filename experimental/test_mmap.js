const args = process.argv;
const mmap = require('mmap.js');
const shm  = require('nodeshm');
const SHM_FILE_NAME=args[args.length-1];


let fd = shm.shm_open(SHM_FILE_NAME, shm.O_RDWR, 600);
if (fd == -1){
    console.log("FD COULD NOT BE OPENED!");
    throw "here";
}



let mm = mmap.alloc(1024, mmap.PROT_READ | mmap.PROT_WRITE, mmap.MAP_SHARED, fd, 0);


console.log("FROM NODE: ", mm.slice(0, mm.lastIndexOf('\n')).toString('utf-8'));