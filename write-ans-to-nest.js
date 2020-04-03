
//https://www.mydatahack.com/bulk-loading-postgres-with-node-js/
var fs = require('fs');
var util = require('util');
const output = fs.createWriteStream('./log/process.log', { flags: 'a' });
const errorOutput = fs.createWriteStream('./log/error.log', { flags: 'a' });
const path = require('path');
const { Pool, Client} = require('pg');
const copyFrom = require('pg-copy-streams').from;

let rawConn = fs.readFileSync('conn.json'); 
let runConn = JSON.parse(rawConn);
const crypt = require('./crypto').EncryptObj;
const decrypt = require('./crypto').DecryptObj;
 
// inputfile & target table
var inputFile =  '';//path.join(__dirname, '/data/WedFeb272019Delfos.csv');
var destFile = '';//'./history/TueFeb272019Delfos.csv'
var table = 'usermanaged.customers'

// Getting connectin parameters from config.json
const host = decrypt(runConn.host);
const user = decrypt(runConn.user);
const pw = decrypt(runConn.pw);
const db = decrypt(runConn.db);
const port = decrypt(runConn.port);
const conString = `postgres://${user}:${pw}@${host}:${port}/${db}`


console.log = function(...args) {
    for(var x=0;x<args.length;x++){
        if(Object.getPrototypeOf( args[x] ) === Object.prototype){
            args[x] = JSON.stringify(args[x]);
        }
    }
    var op = args.join(' ');
    output.write(new Date().toLocaleString() + ' -[WRITEANSTONEST]- ' + util.format(op) + '\r\n');
};

console.error = function(...args) {
    for(var x=0;x<args.length;x++){
        if(Object.getPrototypeOf( args[x] ) === Object.prototype){
            args[x] = JSON.stringify(args[x]);
        }
    }
    var op = args.join(' ');
    errorOutput.write(new Date().toLocaleString() + ' -[WRITEANSTONEST]- ' + util.format(op) + '\r\n');
};

// Connecting to Database
const client = new Client({
    connectionString: conString,
  })
 
client.connect()


 const executeAllTasks = (targetTable) =>  {
    console.log('Starting executeAllTasks', inputFile)
    return new Promise(function(resolve, reject) {
        setTimeout(function () {
            const execute = (target, callback) => {
                client.query(`Truncate ${target}`, (err) => {
                    if (err) {
                        client.end()
                        callback(err)
                        // return console.log(err.stack)
                    } else {
                        console.log(`Truncated ${target}`)
                        callback(null, target)
                    }
                })
            }
            execute(targetTable, (err) =>{
                if (err) return console.error(`Error in Truncate Table: ${err}`)
                var stream = client.query(copyFrom(`COPY ${targetTable} FROM STDIN CSV DELIMITER ';' HEADER`));
                var fileStream = fs.createReadStream(inputFile, { encoding: 'utf8' });
                fileStream.on('error', (error) =>{
                    console.error(`Error in creating read stream ${error} on ${inputFile}`);
                    reject(error);
                })
                stream.on('error', (error) => {
                    console.error(`Error in creating stream ${error} on ${inputFile}`);
                    reject(error);
                })
                stream.on('end', () => {
                    console.log(`Completed loading data into ${targetTable}`)
                    const query = client.query('select prProcessANS();', function(err) {
                        if (err) {
                          // same thing - probably need done(err) in here
                          console.error(`Error reading from ${targetTable} on ${inputFile}`,err.message);      
                          reject(err);
                        }
                    });
                    query.on('end', () => {
                        console.log(`Process on Database is completed and data were read from ${targetTable}`);
                        console.log('Preparing to move ', inputFile ,' to ', destFile);
                        new Promise(function(resolve, reject) {
                            setTimeout(function () {
                                fs.rename(inputFile, destFile, function (err) {
                                    if (err){
                                        console.log('Error: Check error log to details ', destFile , ' doesn´t be moved')
                                        console.error('Can not move file ', inputFile, ' to ', destFile,' Description:', err);
                                    }else{
                                        console.log('Successfully ', destFile , ' moved')
                                    } 
                                  })
                            },1000)
                        }); 
                    });
                    client.end()
                    resolve('Promisse resolved on ', inputFile)
                })
                fileStream.pipe(stream);
            }) 
            resolve('OK');
        }, 1000);        

        
    });
}

const  readFilesinData =  () =>{

    fs.readdir('./data/', (err, files) => {
        var file = files[0];
        //files.forEach(file => {
        if(file){
            console.log('Starting to read ' + file)
            processFile( './data/' + file, './history/' + file);
        }else{
            console.log('There are no files to be read');
            client.end()
        }
        //});
      });
      return true;
}

 function processFile(pinputFile, pdestFile){
    inputFile = pinputFile;  
    destFile = pdestFile;  
    executeAllTasks('tempansvidasplanos')
}
// Execute the function
//executeQuery('temp_indger');

readFilesinData();