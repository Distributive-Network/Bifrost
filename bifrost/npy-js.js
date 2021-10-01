/**
 *  @file       npy-js.js
 *
 *  The purpose of this library is to allow the user to read and write *.npy files,
 *  so long as they are standard and not compressed or pickled.
 *
 *  This javascript utility builds off the work of Nicholas Tancredi.
 *  (https://github.com/NicholasTancredi/read-npy-file)
 *
 *  We simply use this library as a tool to read npy files and have added functionality to write these npy files back to disk if needed.
 *
 *  @author     Hamada Gasmallah
 *  @date       Nov 2020
 */

/** @type {*} */
const { writeFileSync, readFileSync, existsSync } = require('fs');

/**
 *
 * Converts an ascii string to a dataview.
 *
 * @param {String} ascii
 * @return {DataView} Dataview 
 */
const asciiToDataView = (ascii)=>{
  let binArray = new Uint8Array(ascii.length);
  let dv = new DataView(binArray.buffer); 
  for (let i = 0; i < ascii.length; i++){
    dv.setUint8(i, ascii.charCodeAt(i));
  }
  return dv;
};

/**
 * 
 * Converts a dataview to an ascii string
 *
 * @param {DataView} dv
 * @return {String} ascii 
 */
const dataViewToAscii = (dv)=>{
  let out = "";
  for (let i = 0; i < dv.byteLength; i++){
    const val = dv.getUint8(i);
    if (val===0){
      break;
    }
    out += String.fromCharCode(val);
  }
  return out;
};

/**
 * Convert an arrayBuffer to a buffer by using a uint8arr as an intermediate
 * 
 * @param {ArrayBuffer} ab
 * @return {Buffer} 
 */
const arrayBufferToBuffer = (ab)=>{
  var buf = Buffer.alloc(ab.byteLength);
  var view = new Uint8Array(ab);
  for (var i=0; i< buf.length; ++i){
    buf[i] = view[i];
  }
  return buf;
};

/**
 *
 * Converst a Buffer to an arraybuffer
 *
 * @param {Buffer} buff
 * @return {ArrayBuffer} 
 */
const bufferToArrayBuffer = (buff)=>{
  return buff.buffer.slice(buff.byteOffset, buff.byteOffset + buff.byteLength);
};

/**
 *
 * Reduces the shape to the number of elements ([1,100,228,4] => 91200)
 *
 * @param {Array} shape
 * @return {Number} 
 */
const numEls = (shape)=>{
  if (shape.length===0){
    return 1;
  }else{
    return shape.reduce((a, b)=> a*b);
  }
};

/**
 *
 * Helper function to write arraybuffer to file
 * 
 * @param {String} filename
 * @param {ArrayBuffer} ab
 * @return {undefined} 
 */
const write = (filename,ab)=>{
  const buf = arrayBufferToBuffer(ab);
  writeFileSync( filename, buf );
  return;
};

/**
 *
 * Helper function to read a file and get the array buffer out.
 * Will throw if the file has less than 6 bytes.
 * @param {String} filename
 * @return {ArrayBuffer}  
 */
const read = (filename) => {
  const ret = bufferToArrayBuffer(readFileSync(filename));
  if (ret.byteLength <=5){
    throw RangeError(
      `Invalid 'byteLength'='${ret.byteLength}' for a numpy file. It's likely empty.`
    );
  };
  return ret;
};

//map the numpy datatype to the typed array
/** @type {dtypeTypedArrayMap} */
const dtypeTypedArrayMap = {
  'f8': s => new Float64Array(s),
  'f4': s => new Float32Array(s),
  'i8': s => new BigInt64Array(s),
  'i4': s => new Int32Array(s),
  'i2': s => new Int16Array(s),
  'i1': s => new Int8Array(s),
  'u8': s => new BigUint64Array(s),
  'u4': s => new Uint32Array(s),
  'u2': s => new Uint16Array(s),
  'u1': s => new Uint8Array(s),
};

//map the datatypes to the string rep 
/** @type {dtypeTypedMap} */
const dtypeTypedMap = {
  'f8': 'float64', 
  'f4': 'float32',
  'i8': 'int64'  ,
  'i4': 'int32'  ,
  'i2': 'int16'  ,
  'i1': 'int8'   ,
  'u8': 'uint64' ,
  'u4': 'uint32' ,
  'u2': 'uint16' ,
  'u1': 'uint8'
};

/**
 * buildDataArray Builds a DataArray out of a typed array and a shape. 
 *
 * @param typedArray some arrayview on an arraybuffer or an arraybuffer itself.
 * @param shape=[]  The shape of the array to be interpreted
 * @returns {DataArray}
 */
function buildDataArray(typedArray, shape=[]){
  if (typedArray.constructor.name === 'ArrayBuffer'){
    typedArray = new Uint8Array(typedArray);
  }
  //check that we have a valid type
  if (ArrayBuffer.isView(typedArray)){
    //check that we have the right number of elements
    if (numEls(shape) == typedArray.length){
      let possibleTypes = Object.values(dtypeTypedMap);
      let typedArrayConstructor = typedArray.constructor.name.toLowerCase();
      let ind = -1;
      for (let i = 0; i < possibleTypes.length; i++){
        if (typedArrayConstructor.includes(possibleTypes[i])){
          ind=i;
        }
      }
      if (ind==-1){
        throw new TypeError(`Argument passed is a typedarray for which we have no construction....`);
      }

      let dtype = Object.keys(dtypeTypedMap)[ind];
      let byteorder = (dtype[dtype.length-1] == '1') ? '|' : '=';
      let descr = byteorder + dtype;

      let strShape = '(';
      for (let i = 0; i < shape.length-1; i++){
        strShape += shape[i] + ', ';
      }
      strShape += shape[shape.length-1] + ')';
      
      let headerStr = String(`{'descr': '${descr}', 'fortran_order': False, 'shape': ${strShape}, }                                                  \n`);
      
      return new DataArray(typedArray, descr, shape, headerStr);

    }else{
      throw "Wrong shape passed: " + numEls(shape).toString() + " cannot be parsed as : " + shape.toString();
    }
  }else{
    throw new TypeError(`Argument passed is not a typedArray or an arrayBuffer. It is a ${typeof typedArray}`);
  };
};

/**
 *
 * The data array class contains the typed array of the numpy file.
 * It also contains information about the dtype and the shape. 
 * 
 * It is critical that the shape and the datatypes are not changed.
 * If they are, the header is incorrect and must be fixed.
 *
 * @class DataArray
 * @param {ArrayBuffer} arrayBuffer - The arraybuffer containing the data of the numpy array
 * @param {string} dtype - The byte representation of the numpy array as a string (ex: '<i2' -> int16)
 * @param {Array} shape - An array of the shape (ex: [1,100,100,2])
 * @param {string} header - The numpy header for this specific data array in ascii. 
 * @property {string} this.header The numpy header for this specific data array.
 * @property {string} this.dtype The string representation of the datatype
 * @property {TypedArray} this.typedArray The typed array of the numpy file
 * @property {Array} this.shape The shape of the numpy array
 */
class DataArray {
  constructor(arrayBuffer, dtype, shape, header){
    this.header = header;
    this.dtype = dtypeTypedMap[dtype];
    this.typedArray = dtypeTypedArrayMap[dtype](arrayBuffer);
    this.shape = shape;
  };
};

/**
 *
 * Finds the key in the object that gives the value requested.
 *
 * @param {Object} object
 * @param {*} value
 * @return {string} 
 */
function getKeyByValue(object, value) {
    return Object.keys(object).find(key => object[key] === value);
}

/**
 *
 * Write a numpy file based on a dataArray
 *
 * @param {string} filename
 * @param {DataArray} dataArray
 */
const writeNumpyFile = (filename, dataArray)=>{
  let dv = unparseNumpyFile(dataArray);
  write(filename, dv.buffer);
};

/**
 * Convert a dataarray to a dataview representation that aligns with the numpy spec
 * 
 * @param {DataArray} dataArray
 * @return {DataView} dv 
 */
const unparseNumpyFile = (dataArray)=>{
  let totalLengthOfDV = 0;
  let abValues = new DataView(dataArray.typedArray.buffer);
  totalLengthOfDV += abValues.byteLength;

  let dtype = getKeyByValue(dtypeTypedMap, dataArray.dtype);
  let shape = dataArray.shape;

  const byte0 = 0x93; //1
  totalLengthOfDV += 1;
  const magicStr = asciiToDataView("NUMPY"); // 5
  totalLengthOfDV += magicStr.byteLength;

  let headerDV = asciiToDataView(dataArray.header);
  totalLengthOfDV += headerDV.byteLength;
  let headerLen = headerDV.byteLength;
  totalLengthOfDV += 2;
  const vMajor = 1;
  const vMinor = 0;
  totalLengthOfDV += 2;
  
  let binArray = new Uint8Array(totalLengthOfDV);
  let dv = new DataView(binArray.buffer); 

  let pos = 0;
  dv.setUint8(pos++, byte0);
  for (let i = 0; i < 5; i++){
    dv.setUint8(pos++, magicStr.getUint8(i));
  };

  dv.setUint8(pos++, vMajor);
  dv.setUint8(pos++, vMinor);
  
  dv.setUint16(pos, headerLen, true);
  pos += 2;

  for (let i = 0; i < headerLen; i++){
    dv.setUint8(pos++, headerDV.getUint8(i) );  
  };

  for (let i=0; i<abValues.byteLength;i++){
    dv.setUint8(pos++, abValues.getUint8(i));
  };

  return dv;
};

/**
 *
 * Read a numpy file based on a dataArray
 *
 * @param {string} filename
 * @return {DataArray} dataArray
 */
const readNumpyFile = (filename)=>{
  const ab = read(filename);
  return parseNumpyFile(ab);
};

/**
 * Convert an arraybuffer to a dataarray representation based on the numpy spec
 * 
 * @param {ArrayBuffer} ab
 * @return {DataArray} da 
 */
const parseNumpyFile= (ab)=>{

  const view = new DataView(ab);
  let pos=0;

  const byte0 = view.getUint8(pos++);
  const magicStr = dataViewToAscii(new DataView(ab, pos, 5));
  pos += 5;

  if (byte0 !== 0x93 || magicStr !== "NUMPY"){
    throw TypeError("Note a Numpy file.");
  };

  const version = [ view.getUint8(pos++), view.getUint8(pos++)].join(".")
  if (version !== "1.0"){
    throw Error("Unsupported version. Version found is: " + version);
  }

  const headerLen = view.getUint16(pos, true);
  pos+=2;

  const headerPy = dataViewToAscii(new DataView(ab, pos, headerLen));
  pos += headerLen;
  const bytesLeft = view.byteLength - pos;
 
  const headerJson = headerPy
    .replace("True", "true")
    .replace("False", "false")
    .replace(/'/g, `"`)
    .replace(/,\s*}/, " }")
    .replace(/,?\)/, "]")
    .replace("(", "[") 

  const header = JSON.parse(headerJson);

  const { shape, fortran_order, descr } = header;

  const dtype = descr.slice(1);

  if (fortran_order){
    throw Error("NPY Parse error. No implementation for fortran_order type");
  }
  
  if (bytesLeft !== numEls(shape) * parseInt(dtype[dtype.length-1])) {
    throw RangeError("Invalid bytes for numpy dtype");
  }

  if (!(dtype in dtypeTypedArrayMap)){
    throw Error(`Unknown dtype "${dtype}". Either invalid or needs javascript implementation.`);
  }

  return new DataArray(ab.slice(pos, pos+bytesLeft), dtype, shape, headerPy);
};

exports.buildDataArray = buildDataArray;
exports.parseNumpyFile = parseNumpyFile;
exports.unparseNumpyFile = unparseNumpyFile;
exports.readNumpyFile = readNumpyFile;
exports.writeNumpyFile = writeNumpyFile;
