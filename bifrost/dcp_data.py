from bifrost import node, npm

node.run("""

const loadString = `

    const loadShards = async function loadShards(packageName) {

      pako = require('pako');

      try {

        async function _requirePackage(myPackageName) {

            const thisPackage = await require(myPackageName);

            return thisPackage.SHARD_DATA;
        }

        async function _loadBinary(base64String) {

            let binaryString = await atob(base64String);
            
            const binaryLength = binaryString.length;

            let binaryArray = new Uint8Array(binaryLength);
            for(let i = 0; i < binaryLength; i++) {
              binaryArray[i] = binaryString.charCodeAt(i);
            }
            return binaryArray;
        }

        const shardName = NAME + '-' + packageName;
        const shardPath = NAME + '/' + shardName;

        await new Promise((resolve, reject) => {

            try {

                module.provide([shardPath], () => {

                    resolve();
                });

            } catch(error) {

                reject(error);
            }
        });

        let shardData = await _requirePackage(shardName);

        shardData = await _loadBinary(shardData);

        shardData = await pako.inflate(shardData, { to: 'string' });

        shardData = await JSON.parse(shardData);

        progress();

        return shardData;

      } catch (e) {

        throw(e);
      }
  };

  exports.load = loadShards;

`;

let dcpDataPublish = async function dataPublish(
  datasetArray,
  packageName,
  datasetShards
) {

  if (!fs.existsSync('out')) fs.mkdirSync('out');

  const packageDirectory = 'out/' + packageName;

  if (!fs.existsSync(packageDirectory)) fs.mkdirSync(packageDirectory);

  const dcpLazy = loadString;

  async function streamWrapper() {

      let myStream = fs.createWriteStream('' + packageDirectory + '/' + packageName + '.js');

      let streamPromise = new Promise(function(resolve, reject) {

          myStream.once('open', function(fd) {
              myStream.write(`module.declare(function(require,exports,module){`);
              myStream.write(`const NAME = "`);
              myStream.write(packageName);
              myStream.write(`";`);
              myStream.write(dcpLazy);
              myStream.write(`});`);
              myStream.end();
          });

          myStream.once('finish', function(fd) {

              resolve();
          });

          myStream.once('error', reject);
      });
      await streamPromise;
  };
  await streamWrapper();

  async function publishPackage(fileNameArray, myPackageName) {

    const packageDcp = {
      name: myPackageName,
      version: '0.1.0',
      files: {}
    };

    packageDcp.files[process.cwd() + '/' + packageDirectory + '/' + myPackageName + '.js'] = myPackageName + '.js';

    for (let i = 0; i < fileNameArray.length; i++) {

      packageDcp.files[process.cwd() + '/' + packageDirectory + '/' + fileNameArray[i]] = fileNameArray[i];
    }
    
    const packageString = await JSON.stringify(packageDcp);

    async function packageWrapper() {

        let myStream = fs.createWriteStream('' + packageDirectory + '/package.dcp');
        let streamPromise = new Promise(function(resolve, reject) {

            myStream.once('open', function(fd) {
                myStream.write(packageString);
                myStream.end();
            });

            myStream.once('finish', function(fd) {

                resolve();
            });

            myStream.once('error', reject);
        });
        await streamPromise;
    };
    await packageWrapper();
    
    try {

        async function publishWrapper() {

          try {

              await require('dcp/publish').publish('' + packageDirectory + '/package.dcp');

            } catch (clientError) {

                console.log('clientError');
                console.log(clientError);
            }
        };
        await publishWrapper();

    } catch (publishError) {

        console.log('publishError');
        console.log(publishError);
    }
  }
  
  async function startLoader(myInput, myName, dataShards = 10, myShardSize = 4000000) {

      let moduleWrapper = {};
      moduleWrapper.openDeclare = `module.declare(function(require,exports,module){`;
      moduleWrapper.openExports = `exports.SHARD_DATA = '`;
      moduleWrapper.closeExports = `';`;
      moduleWrapper.openName = `exports.SHARD_NAME = '`;
      moduleWrapper.closeName = `';`;
      moduleWrapper.closeDeclare = `});`;

      let arrayShardSize = Math.ceil(myInput.length / dataShards);

      let myNameArray = [];

      for (let i = 0; i < dataShards; i++) {

          const shardName = `${myName}-shard-${i}.js`;
          myNameArray.push(shardName);

          let shardStart = i * arrayShardSize;
          let shardStop = shardStart + arrayShardSize;

          let thisShard = myInput.slice(shardStart, shardStop);
          thisShard = pako.deflate(JSON.stringify(thisShard));
          thisShard = Buffer.from(thisShard).toString('base64');

          async function shardWrapper() {

              let shardStream = fs.createWriteStream('' + packageDirectory + '/' + shardName);
              let shardPromise = new Promise(function(resolve, reject) {

                  shardStream.once('open', function(fd) {

                      shardStream.write(moduleWrapper.openDeclare);
                      shardStream.write(moduleWrapper.openExports);
                      shardStream.write(thisShard);
                      shardStream.write(moduleWrapper.closeExports);
                      shardStream.write(moduleWrapper.openName);
                      shardStream.write(shardName);
                      shardStream.write(moduleWrapper.closeName);
                      shardStream.write(moduleWrapper.closeDeclare);
                      shardStream.end();
                  });

                  shardStream.once('finish', function(fd) {
                      resolve();
                  });

                  shardStream.once('error', reject);
              });
              await shardPromise;
          };
          await shardWrapper();
      }

      return myNameArray;
  }

  let nameArray = [];

  let theseNames = await startLoader(datasetArray, packageName, datasetShards);
  nameArray = nameArray.concat(theseNames);

  await publishPackage(nameArray, packageName);

  return nameArray;

}

""")


def dcp_data_publish(
  dataset_array,
  package_name,
  dataset_shards
):

  publish_parameters = {
    'datasetArray': dataset_array,
    'packageName': package_name,
    'datasetShards': dataset_shards,
  }

  node.run("""

  let myNameArray = dcpDataPublish(datasetArray, packageName, datasetShards);

  return myNameArray;

  """, publish_parameters)
