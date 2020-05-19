//https://ourcodeworld.com/articles/read/133/how-to-create-a-sftp-client-with-node-js-ssh2-in-electron-framework
var fs = require('fs');
var ClientSSH = require('ssh2').Client;
const { Pool, Client} = require('pg')
var util = require('util');
//var Regex = require("regex");
const output = fs.createWriteStream('./log/process.log', { flags: 'a' });
const errorOutput = fs.createWriteStream('./log/error.log', { flags: 'a' });
const datReg = new RegExp( /^((19|2\d)\d\d)-((0?[1-9])|(1[0-2]))-((0?[1-9])|([12]\d)|(3[01]))/);
let rawdata = fs.readFileSync('parameter.json');  
let rawConn = fs.readFileSync('conn.json'); 
let runParam = JSON.parse(rawdata);
let runConn = JSON.parse(rawConn);
const configDB = require('./config.json')
const crypt = require('./crypto').EncryptObj;
const decrypt = require('./crypto').DecryptObj;

// Getting connectin parameters from config.json
/*const host = configDB.host
 const user = configDB.user
 const pw = configDB.pw
 const db = configDB.db
 const port = configDB.port
*/

const host = decrypt(runConn.host);
const user = decrypt(runConn.user);
const pw = decrypt(runConn.pw);
const db = decrypt(runConn.db);
const port = decrypt(runConn.port);

const conString = `postgres://${user}:${pw}@${host}:${port}/${db}`


/*
and ae.dt_data >= (to_char(now()::date,'YYYYMM01')::date - interval '2 month')::date and ae.dt_data < to_char(now()::date,'YYYYMM01')::date
*/
console.log = function(...args) {
    for(var x=0;x<args.length;x++){
        if(Object.getPrototypeOf( args[x] ) === Object.prototype){
            args[x] = JSON.stringify(args[x]);
        }
    }
    var op = args.join(' ');
    output.write(new Date().toLocaleString() + ' -[COPYFROMSERVER]- ' + util.format(op) + '\r\n');
};

console.error = function(...args) {
    for(var x=0;x<args.length;x++){
        if(Object.getPrototypeOf( args[x] ) === Object.prototype){
            args[x] = JSON.stringify(args[x]);
        }
    }
    var op = args.join(' ');
    errorOutput.write(new Date().toLocaleString() + ' -[COPYFROMSERVER]- ' + util.format(op) + '\r\n');
};


var config = {
    1:{
      idClient: 1  ,
      name: 'SUSGA-HML',
      connSettings : {
        host: 'susga.zapto.org',
        port: 2322, // Normal is 22 port
        username: 'dicomvix',
        password: 'system98'
        // You can use a key file too, read the ssh2 documentation
      },
      remoteDir:'/home/dicomvix/preparo/',
      psql : 'psql -U dicomvix -d homologacao -c'
    }
}

var args = process.argv.slice(2);

var clientData = config[args[0]];



function downloadDirSSH(){
    var remotePathToList =  clientData.remoteDir;
    console.log('Testando ssh...', clientData.connSettings );
    var conn = new ClientSSH();
    console.log('New Client OK')
    conn.on('ready', function() {
        conn.sftp(function(err, sftp) {
            if (err) throw err;
            
            sftp.readdir(remotePathToList, function(err, list) {
                    if (err){
                        console.log('Erro ao ler o diretótio ', remotePathToList, err)
                        //throw err;
                    }else{
                        // List the directory in the console
                        list.forEach(l => {
                            var moveFrom = remotePathToList + '' + l.filename;
                            var moveTo = "./data/" + l.filename;
                            console.log("Preparing DownLoad ", moveFrom, ' to ', moveTo);
                            
                            sftp.fastGet(moveFrom, moveTo , {}, function(downloadError){
                                if(downloadError) 
                                {
                                    console.error("Erro: ", downloadError);
                                    throw downloadError;
                                    
                                }else{
                                    console.log("Succesfully DownLoaded");
                                }
                            });
    
                        });
                        // Do not forget to close the connection, otherwise you'll get troubles
                        //conn.end();
                    } 

            });
        });

    }).connect(clientData.connSettings);
}

function testSSH(){
    var remotePathToList = clientData.remoteDir;
    console.log('Testando ssh...', clientData.connSettings )
    var conn = new ClientSSH();
    console.log('New Client OK')
    conn.on('ready', function() {
        conn.sftp(function(err, sftp) {
            if (err) throw err;
            
            sftp.readdir(remotePathToList, function(err, list) {
                    if (err){
                        console.log('Erro ao ler o diretótio ', remotePathToList, err)
                        //throw err;
                    }else{
                        // List the directory in the console
                        list.forEach(l => {
                            console.log(l.filename);
                        });
                        //console.dir(list);
                    } 
                    // Do not forget to close the connection, otherwise you'll get troubles
                    conn.end();
            });
        });
    }).connect(clientData.connSettings);
}

function sshDownloadFile(){
    console.log("Conectando SSH", clientData.connSettings);
    var conn = new ClientSSH();
    conn.on('ready', function() {
        conn.sftp(function(err, sftp) {
            if (err) throw err;
            
            var moveFrom = "/home/dicomvix/000010000100020.rtf";
            var moveTo = "./data/000010000100020.rtf";
            //C:\projetos\BI\NEST\Copy-Data\data\customer.csv
            console.log("Movendo ", moveFrom , " para ", moveTo);
            sftp.fastGet(moveFrom, moveTo , {}, function(downloadError){
                if(downloadError) throw downloadError;
    
                console.log("Succesfully DownLoaded");
            });
            // Do not forget to close the connection, otherwise you'll get troubles
            //conn.end();
        });
    }).connect(clientData.connSettings);

}

function sshUploadFile(){
    var conn = new ClientSSH();
    conn.on('ready', function() {
        conn.sftp(function(err, sftp) {
            if (err) throw err;
            
            var fs = require("fs"); // Use node filesystem
            var readStream = fs.createReadStream( "path-to-local-file.txt" );
            var writeStream = sftp.createWriteStream( "path-to-remote-file.txt" );

            writeStream.on('close',function () {
                console.log( "- file transferred succesfully" );
            });

            writeStream.on('end', function () {
                console.log( "sftp connection closed" );
                conn.close();
            });

            // initiate transfer of file
            readStream.pipe( writeStream );
        });
    }).connect(connSettings);
}

function removeFileSSH(){
    var remotePathToList = '/tmp/feriados.csv';

    var conn = new ClientSSH();
    conn.on('ready', function() {
        conn.sftp(function(err, sftp) {
             if (err) throw err;
             
             sftp.unlink(remotePathToList, function(err){
                if ( err ) {
                    console.log( "Error, problem starting SFTP: %s", err );
                }
                else
                {
                    console.log(remotePathToList, " file unlinked" );
                }
    
                conn.end();
            });
        });
    }).connect(connSettings);

}

function debugConfig(){
   
    Object.keys(config).forEach(function(key) {
        console.log('\'',config[key].remoteDir,'\',\'',config[key].psql,'\' where idClient = ', config[key].idClient);
    });
  
};

const ExecuteTaskInHost = ()=>{

    console.log("Connection String:  " ,  crypt(conString));
    var sQuery = 'SELECT * FROM prgetjsonclienthost(' + args + ')';
    clientPG.query(sQuery).then(res => {

        const data = res.rows;
    
        console.log('Dados de Acesso ao Cliente:', sQuery);
        data.forEach(row => {
            var result = JSON.stringify(row.prgetjsonclienthost);
            //console.log('Linha retornada : ', result);
            clientData = JSON.parse(result);
            connSettings = clientData.connSettings;
            //console.log('Param: ', clientData);
            //console.log('Client Connection String: ', connSettings);
        })
    
        console.log('Configurações obtidas com sucesso:');
        writeFileDBSSH();
    }).catch(err => {
        console.error('Erro ao obter parâmetros de acesso ao cliente:',err.stack);
    }).finally(() => {
        clientPG.end()
    });
    
};

function WriteDebugTest(){

    console.log("Host = ", decrypt(runConn.host));
    console.log("user = ", decrypt(runConn.user));
    console.log("pw = ", decrypt(runConn.pw));
    console.log("db = ", decrypt(runConn.db));
    console.log("port = ", decrypt(runConn.port));
    
    var mconString = `postgres://${user}:${pw}@${host}:${port}/${db}`;
    console.log("conString = ", mconString);
}


//WriteDebugTest();

downloadDirSSH();


//writeFileDBSSH();



//debugConfig();

