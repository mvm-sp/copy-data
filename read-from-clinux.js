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
let runParam = JSON.parse(rawdata);
const configDB = require('./config.json')

// Getting connectin parameters from config.json
const host = configDB.host
const user = configDB.user
const pw = configDB.pw
const db = configDB.db
const port = configDB.port
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

/*
var config = {
    1:{
      idClient: 1  ,
      name: 'delfos',
      connSettings : {
        host: 'clinicadelfos.zapto.org',
        port: 1157, // Normal is 22 port
        username: 'dicomvix',
        password: 'SysteM98'
        // You can use a key file too, read the ssh2 documentation
      },
      remoteDir:'/usr/home/dicomvix/move/delfos.csv',
      psql : 'psql -U dicomvix -d clinux_delfos -c'
    },
    2:{
        idClient: 2  ,
        name: 'bertinetti',        
        connSettings : {
          host: '191.33.180.195',
          port: 1157, // Normal is 22 port
          username: 'dicomvix',
          password: 'system98'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/usr/home/dicomvix/move/bertinetti.csv',
        psql : 'psql -U dicomvix -d clinux_bertinetti -c'
    },
    3:{
        idClient: 3  ,
        name: 'cdi-uberlandia',        
        connSettings : {
          host: 'cdiub.zapto.org',
          port: 1157, // Normal is 22 port
          username: 'dicomvix',
          password: 'system98'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/usr/home/dicomvix/move/cdiuberlandia.csv',
        psql : 'psql -U dicomvix -d clinux_cdi -c'
    },
    4:{
        idClient: 4  ,
        name: 'cdisalvador',        
        connSettings : {
          host: 'cdisalvador.zapto.org',
          port: 22, // Normal is 22 port
          username: 'dicomvix',
          password: 'system98'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/usr/home/dicomvix/move/cdisalvador.csv',
        psql : 'psql -U dicomvix -d clinux_salvador -c'
    },
    5:{
        idClient: 5  ,
        name: 'saosalvador',        
        connSettings : {
          host: 'clinicasaosalvador.ddns.net',
          port: 1157, // Normal is 22 port
          username: 'dicomvix',
          password: 'GTESSA@2018'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/usr/home/dicomvix/move/saosalvador.csv',
        psql : 'psql -U dicomvix -d clinux_saosalvador -c'
    },
    6:{
        idClient: 6  ,
        name: 'cepem',        
        connSettings : {
          host: '177.124.226.98',
          port: 527, // Normal is 22 port
          username: 'clinuxweb',
          password: 'ClinuX99Fb'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/home/clinuxweb/move/cepem.csv',
        psql : 'psql -U dicomvix -d clinux_cepem -c'
    },
    7:{
        idClient: 7  ,
        name: 'saomarcelo',        
        connSettings : {
          host: 'saomarcelo.zapto.org',
          port: 1157, // Normal is 22 port
          username: 'dicomvix',
          password: 'system98'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/usr/home/dicomvix/move/saomarcelo.csv',
        psql : 'psql -U dicomvix -d clinux_saomarcelo -c'
    },
    8:{
        idClient: 8  ,
        name: 'medicine',        
        connSettings : {
          host: '170.82.182.106',
          port: 39802, // Normal is 22 port
          username: 'dicomvix',
          password: 'system98'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/usr/home/dicomvix/move/medicine.csv',
        psql : 'psql -U dicomvix -d clinux_medicine -c'
    },
    9:{
        idClient: 9  ,
        name: 'magnus',        
        connSettings : {
          host: 'alfenas.zapto.org',
          port: 22, // Normal is 22 port
          username: 'dicomvix',
          password: 'system98'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/usr/home/dicomvix/move/magnus.csv',
        psql : 'psql -U dicomvix -d clinux_alfenas -c'
    },
    10:{
        idClient: 10  ,
        name: 'megaimagen',        
        connSettings : {
          host: '187.92.71.161',
          port: 1157, // Normal is 22 port
          username: 'clinuxweb',
          password: 'ClinuX99Fb'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/usr/home/clinuxweb/move/megaimagen.csv',
        psql : 'psql -U clinux -d clinux_megaimagem -c'
    },
    11:{
        idClient: 11  ,
        name: 'clirad',        
        connSettings : {
          host: 'clirad.zapto.org',
          port: 1157, // Normal is 22 port
          username: 'dicomvix',
          password: 'system98'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/usr/home/dicomvix/move/clirad.csv',
        psql : 'psql -U dicomvix -d clinux_clirad_nova -c'
    },
    12:{
        idClient: 12  ,
        name: 'rd-xavier',        
        connSettings : {
          host: '177.185.141.71',
          port: 1157, // Normal is 22 port
          username: 'dicomvix',
          password: 'system98'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/usr/home/dicomvix/move/rd-xavier.csv',
        psql : 'psql -U dicomvix -d clinux_medimagem -c'
    },
    13:{
        idClient: 13  ,
        name: 'rad-med',        
        connSettings : {
          host: 'radmed.zapto.org',
          port: 1157, // Normal is 22 port
          username: 'dicomvix',
          password: 'system98'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/usr/home/dicomvix/move/rad-med.csv',
        psql : 'psql -U dicomvix -d clinux_radmed -c'
    },
    14:{
        idClient: 14  ,
        name: 'susga',        
        connSettings : {
          host: '177.99.226.20',
          port: 1157, // Normal is 22 port
          username: 'dicomvix',
          password: 'system98'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/usr/home/dicomvix/move/susga.csv',
        psql : 'psql -U dicomvix -d clinux_susga -c'
    },
    15:{
        idClient: 15  ,
        name: 'tomoclinica',        
        connSettings : {
          host: 'webpacs.tomoclinica.com.br',
          port: 2222, // Normal is 22 port
          username: 'dicomvix',
          password: 'system98'
          // You can use a key file too, read the ssh2 documentation
        },
        remoteDir:'/usr/home/dicomvix/move/tomoclinica.csv',
        psql : 'psql -U dicomvix -d clinux_tomoclinica -c'
    }
}
*/
var args = process.argv.slice(2);

//var clientData = config[args[0]];

//Variavel para Acessar cliente
var clientData ;
//Variavel para String de Conexão do Cliente
var connSettings ;

// Connecting to Database
const clientPG = new Client({
    connectionString: conString,
  });
 
clientPG.connect();





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
    console.log('{' + clientData.name  + '}' +'Testing ssh...', connSettings);
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

function downloadDirSSH(){
    var remotePathToList = '/usr/home/dicomvix/move';
    console.log('Testando ssh...', connSettings )
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
                            var moveFrom = remotePathToList + '/' + l.filename;
                            var moveTo = "./data/" + l.filename;
                            console.log("Preparing DownLoad ", moveFrom, ' to ', moveTo);
                            //C:\projetos\BI\NEST\Copy-Data\data\customer.csv
                            sftp.fastGet(moveFrom, moveTo , {}, function(downloadError){
                                if(downloadError){
                                    throw downloadError;
                                }else{
                                    console.log("Succesfully DownLoaded ", l.filename);
                                } 
                            });
    
                        });
                        // Do not forget to close the connection, otherwise you'll get troubles
                        conn.end();
                    } 

            });
        });
    }).connect(connSettings);
}

function testSSH(){
    var remotePathToList = '/usr/home/dicomvix/scripts';
    console.log('Testando ssh...', connSettings )
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
    }).connect(connSettings);
}

function sshDownloadFile(){
    var conn = new ClientSSH();
    conn.on('ready', function() {
        conn.sftp(function(err, sftp) {
            if (err) throw err;
            
            var moveFrom = "/usr/home/dicomvix/scripts.tar.gz";
            var moveTo = "./data/scripts.tar.gz";
            //C:\projetos\BI\NEST\Copy-Data\data\customer.csv
    
            sftp.fastGet(moveFrom, moveTo , {}, function(downloadError){
                if(downloadError) throw downloadError;
    
                console.log("Succesfully DownLoaded");
            });
        });
    }).connect(connSettings);

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

    console.log("Connection String:  " ,  conString);

    clientPG.query('SELECT * FROM prgetjsonclienthost(' + args + ')').then(res => {

        const data = res.rows;
    
        console.log('Dados de Acesso');
        data.forEach(row => {
            var result = JSON.stringify(row.prgetjsonclienthost);
            console.log('Linha retornada : ', result);
            clientData = JSON.parse(result);
            connSettings = clientData.connSettings;
            console.log('Param: ', clientData);
            console.log('Client Connection String: ', connSettings);
        })
    
        console.log('Configurações obtidas com sucesso:');
        writeFileDBSSH();
    }).catch(err => {
        console.error('Erro ao obter parâmetros de acesso ao cliente:',err.stack);
    }).finally(() => {
        clientPG.end()
    });
    
};

ExecuteTaskInHost();
//writeFileDBSSH();



//debugConfig();

