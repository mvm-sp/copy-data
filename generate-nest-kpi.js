
//https://www.mydatahack.com/bulk-loading-postgres-with-node-js/
var fs = require('fs');
var util = require('util');
const output = fs.createWriteStream('./log/process.log', { flags: 'a' });
const errorOutput = fs.createWriteStream('./log/error.log', { flags: 'a' });
const path = require('path')
const { Pool, Client} = require('pg')
const copyFrom = require('pg-copy-streams').from
const config = require('./config.json')
 
// inputfile & target table
var inputFile =  '';//path.join(__dirname, '/data/WedFeb272019Delfos.csv');
var destFile = '';//'./history/TueFeb272019Delfos.csv'

let rawdata = fs.readFileSync('parameter.json');  
let runParam = JSON.parse(rawdata);

// Getting connectin parameters from config.json
const host = config.host
const user = config.user
const pw = config.pw
const db = config.db
const port = config.port
const conString = `postgres://${user}:${pw}@${host}:${port}/${db}`


console.log = function(...args) {
    for(var x=0;x<args.length;x++){
        if(Object.getPrototypeOf( args[x] ) === Object.prototype){
            args[x] = JSON.stringify(args[x]);
        }
    }
    var op = args.join(' ');
    output.write(new Date().toLocaleString() + ' -[GENERATENESTKPI]- ' + util.format(op) + '\r\n');
};

console.error = function(...args) {
    for(var x=0;x<args.length;x++){
        if(Object.getPrototypeOf( args[x] ) === Object.prototype){
            args[x] = JSON.stringify(args[x]);
        }
    }
    var op = args.join(' ');
    errorOutput.write(new Date().toLocaleString() + ' -[GENERATENESTKPI]- ' + util.format(op) + '\r\n');
};

// Connecting to Database
const client = new Client({
    connectionString: conString,
  })
 
client.connect()

const  executeCalcKPI =  () =>{
    var localQuery = `select prWritePerformanceKPIClientMonth(${runParam.Client}, ${runParam.Year},${runParam.Month});`;
    console.log("Trying to execute...", localQuery);
    return new Promise(function(resolve, reject) {
        setTimeout(function () {
            const execute = (target, callback) => {
                client.query(localQuery, (err) => {
                    if (err) {
                        console.error("An error has ocurred...", err)
                        
                        //callback(err)
                        // return console.log(err.stack)
                    } else {
                        console.log(`Client ${runParam.Client} at ${runParam.Month}/${runParam.Year} has been finished sucessfully..`)
                        //callback(null, target)
                    }
                    client.end();
                })
            }
            execute(localQuery, (err) =>{
                if (err) {
                    return console.error(`Error in executeCalcKPI: ${err}`)
                }else{
                    return console.log(`executeCalcKPI has finished sucessfully, query: `, localQuery)
                }
                client.end();
            }) 
            resolve('OK');
        }, 1000);        
    });
}

 
executeCalcKPI();