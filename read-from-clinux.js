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
let copyTo = require('pg-copy-streams').to;


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

console.log = function(...args) {
    for(var x=0;x<args.length;x++){
        if(Object.getPrototypeOf( args[x] ) === Object.prototype){
            args[x] = JSON.stringify(args[x]);
        }
    }
    var op = args.join(' ');
    output.write(new Date().toLocaleString() + ' -[READFROMCLINUX]- ' + util.format(op) + '\r\n');
};

console.error = function(...args) {
    for(var x=0;x<args.length;x++){
        if(Object.getPrototypeOf( args[x] ) === Object.prototype){
            args[x] = JSON.stringify(args[x]);
        }
    }
    var op = args.join(' ');
    errorOutput.write(new Date().toLocaleString() + ' -[READFROMCLINUX]- ' + util.format(op) + '\r\n');
};

var args = process.argv.slice(2);

//var clientData = config[args[0]];

//Variavel para Acessar cliente
var clientData ;
//Variavel para String de Conexão do Cliente
var connSettings ;

//Connecting to Database
const clientPG = new Client({
    connectionString: conString,
});
 
clientPG.connect();

function writeFileDBDirect(){

    console.log('{' + clientData.name  + '}' +'Begin writeFileDBSSH...');
    var _sqlParam = '';
    var localQuery = fs.readFileSync('./sql/read-from-clinux.sql', 'utf8');
    localQuery = localQuery.replace('{idClient}', clientData.idClient);
    console.log('{' + clientData.name  + '}' + ' Parameter DateIni ',runParam.DateIni,' Parameter DateFin ',runParam.DateFin);
    if(datReg.test(runParam.DateIni) ){
        _sqlParam = ' and ae.dt_data >= \'' + runParam.DateIni +' 00:00:01\'::date  ';
    }
    if(datReg.test(runParam.DateFin) ){
        _sqlParam = _sqlParam + ' and ae.dt_data <= \'' +  runParam.DateFin + ' 23:59:59\'::date  ';
    }
    if(_sqlParam == ''){
        _sqlParam = 'and ae.dt_data >= (to_char(now()::date,\'YYYYMM01\')::date - interval \'2 month\')::date and ae.dt_data < to_char(now()::date,\'YYYYMM01\')::date';
    }
    localQuery = localQuery + _sqlParam;

    console.log('{' + clientData.name  + '}' +'Query Parameter...', _sqlParam);    
    var remotePath = "./data/" + new Date().toDateString().replace(/\s/g, '') + clientData.name + ".csv";

    var commandPsql = 'COPY (' + localQuery + ') TO  STDOUT  with CSV DELIMITER \';\' HEADER';
    console.log('{' + clientData.name  + '}' +' Connect using : ', crypt(connSettings));
    
    const pool = new Pool(connSettings);
    var writer = fs.createWriteStream(remotePath);

    pool.connect(function(pgErr, client, done) {
        if(pgErr) {
          //handle error
          console.error('{' + clientData.name  + '}' +'Execute error on pool.connect ',pgErr);
          return;
        }

        var stream = client.query(copyTo(commandPsql));
        var pipe = stream.pipe(writer);
        pipe.on('finish', function () {
            console.log('{' + clientData.name  + '}' +'Finish read pipe data ');
            done();
        });
        stream.on('end', response => {
          console.log('{' + clientData.name  + '}' +'Finish read stream data');
          done();
        });
        stream.on('error', err => {
          console.error('{' + clientData.name  + '}' +'Error on reading stream  ', err);  
          done();
        })
      })

};    

function writeFileDBSSH(){

    console.log('{' + clientData.name  + '}' +'Begin writeFileDBSSH...');
    var _sqlParam = '';
    var localQuery = fs.readFileSync('./sql/read-from-clinux.sql', 'utf8');
    localQuery = localQuery.replace('{idClient}', clientData.idClient);
    console.log('{' + clientData.name  + '}' + ' Parameter DateIni ',runParam.DateIni,' Parameter DateFin ',runParam.DateFin);
    if(datReg.test(runParam.DateIni) ){
        _sqlParam = ' and ae.dt_data >= \'' + runParam.DateIni +' 00:00:01\'::date  ';
    }
    if(datReg.test(runParam.DateFin) ){
        _sqlParam = _sqlParam + ' and ae.dt_data <= \'' +  runParam.DateFin + ' 23:59:59\'::date  ';
    }
    if(_sqlParam == ''){
        _sqlParam = 'and ae.dt_data >= (to_char(now()::date,\'YYYYMM01\')::date - interval \'2 month\')::date and ae.dt_data < to_char(now()::date,\'YYYYMM01\')::date';
    }
    localQuery = localQuery + _sqlParam;
    console.log('{' + clientData.name  + '}' +'Query Parameter...', _sqlParam);    
    var remotePath = clientData.remoteDir;
    var commandPsql = clientData.psql + ' "copy (' + localQuery + ') TO \'' + remotePath + '\'  with CSV DELIMITER \';\' HEADER"';
    console.log('{' + clientData.name  + '}' +'Testing ssh...', crypt(connSettings));
    var conn = new ClientSSH();
    console.log('{' + clientData.name  + '}' +'New Client OK', clientData.name)
    conn.on('ready', function() {
        conn.exec(commandPsql , 
        function(err, stream) {
            if (err) throw err;
            stream.on('data', function(data) {
              console.log('{' + clientData.name  + '}' +'STDOUT: ' + data);   
                conn.sftp(function(err, sftp) {
                    if (err) throw err;
                    var moveFrom = remotePath;
                    var moveTo = "./data/" + new Date().toDateString().replace(/\s/g, '') + clientData.name + ".csv";
                    console.log('{' + clientData.name  + '}' +'preparing remote file ' + moveFrom  + ' moving to ' + moveTo)
                    sftp.fastGet(moveFrom, moveTo , {}, function(downloadError){
                        if(downloadError) console.error('{' + clientData.name  + '}' +'Downloading file error ', downloadError);
                        console.log('{' + clientData.name  + '}' +"Succesfully DownLoaded");
                        sftp.unlink(remotePath, function(err){
                            if ( err ) {
                                console.error('{' + clientData.name  + '}' + "Error, problem starting SFTP: %s", err );
                            }
                            else
                            {
                                console.log('{' + clientData.name  + '}' +remotePath, " file unlinked" );
                            }
                            conn.end();
                        });
                    });
                });
            }).stderr.on('data', function(data){
              console.log('{' + clientData.name  + '}' +'STDERR: ' + data);      
            }).on('exit', function(code, signal) {
              console.log('{' + clientData.name  + '}' +'Exited with code ' + code + ' and signal: ' + signal);
            });  
        })     
    }).connect(connSettings);
};    

const ExecuteTaskInHost = ()=>{

    console.log("Connection String:  " ,  crypt(conString));
    var sQuery = 'SELECT * FROM prgetjsonclienthost(' + args + ')';
    clientPG.query(sQuery).then(res => {

        const data = res.rows;
    
        console.log(' Dados de Acesso ao Cliente:', sQuery);
        data.forEach(row => {
            var result = JSON.stringify(row.prgetjsonclienthost);
            //console.log('Linha retornada : ', result);
            clientData = JSON.parse(result);
            connSettings = clientData.connSettings;
            //console.log('Param: ', clientData);
            //console.log('Client Connection String: ', connSettings);
        })
    
        console.log('Configurações obtidas com sucesso:');
        if(clientData.psql != null){
            console.log('{' + clientData.name  + '}' +' Executando método via SSH');
            writeFileDBSSH();
        }
        else
        {
            console.log('{' + clientData.name  + '}' +' Executando método via conexão direta');
            writeFileDBDirect();
        }
        
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

ExecuteTaskInHost();


//writeFileDBSSH();



//debugConfig();

