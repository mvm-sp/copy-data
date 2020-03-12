const NodeRSA = require('node-rsa');
const config = require('./info.pk.json');
const data = { hello: 'world' };

// Getting connectin parameters from config.json
const publKey = config.publKey;
const privKey = config.privKey;

const key = new NodeRSA();

var EncryptObj = (objSource)=>{
    key.importKey(publKey, 'pkcs8-public-pem');

    
    var encrypted = key.encrypt(objSource, 'base64');
    //console.log('ENCRYPTED:');
    //console.log(encrypted);
    return encrypted;
};

var DecryptObj = (objSource)=>{
    key.importKey(privKey, 'pkcs1-pem');
    var decryptedString = key.decrypt(objSource, 'utf8');
    //console.log('DENCRYPTED:');
    //console.log(decryptedString);
    return decryptedString;
}

module.exports={
    EncryptObj,
    DecryptObj
};
//EncryptObj(data);
//DecryptObj("aUXSfyhrKU2so7pav1/w0Dzxd33BeNYOgFPDyNIO3CLLLWWgylmI4RfkNS5fCFtsitz07A5g0n4v0hrrxFTg6w==");