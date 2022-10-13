


/**
 * _atob  A polyfill for atob
 *
 * @param string
 * @returns {string}
 */
function _atob (string) {
    var b64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
    string = String(string).replace(/[\t\n\f\r ]+/g, ""); 
    // Adding the padding if missing, for semplicity
    string += "==".slice(2 - (string.length & 3));
    var bitmap, result = "", r1, r2, i = 0;
    for (; i < string.length;) {
        bitmap = b64.indexOf(string.charAt(i++)) << 18 | b64.indexOf(string.charAt(i++)) << 12
                | (r1 = b64.indexOf(string.charAt(i++))) << 6 | (r2 = b64.indexOf(string.charAt(i++)));

        result += r1 === 64 ? String.fromCharCode(bitmap >> 16 & 255)
                : r2 === 64 ? String.fromCharCode(bitmap >> 16 & 255, bitmap >> 8 & 255)
                : String.fromCharCode(bitmap >> 16 & 255, bitmap >> 8 & 255, bitmap & 255);
    }
    return result;
}
  
/**
 * strtoab  Turns a string to an array buffer
 *
 * @param {ArrayBuffer} ab - The array buffer.
 * @returns {String} - string
 */
function abtostr(ab){
    var ui8 = new Uint8Array(ab);
    const mss = 9999;
    let segments = [];
    let s = '';
    for (let i = 0; i < ui8.length/mss; i++){
        segments.push(String.fromCharCode.apply(null, ui8.slice( i * mss, (i+1) * mss)));
    };
    s = segments.join('');
    return Buffer.from(s.toString(), 'binary').toString('base64');
};

/**
 * strtoab  Turns a string to an array buffer
 *
 * @param {string} str - The string to convert to an arraybuffer.
 * @returns {ArrayBuffer} - The array buffer.
 */
function strtoab(str){
    let strin = _atob(str);
    var binaryArray = new Uint8Array(strin.length);
    for (let i = 0; i < strin.length; ++i){
      binaryArray[i] = strin.charCodeAt(i);
    };
    return binaryArray.buffer;
};

exports.abtostr   = abtostr;
exports.strtoab   = strtoab;
