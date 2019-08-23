const firstMethod = ()=>{
    let proc = new Promise(function(resolve, reject) {
        setTimeout(function(){
            console.log('First Method completed...')
        } ,5000)
        resolve("Success!");
        // or
        //reject ("Error!");
    });
    return proc;
}
const secondMethod = ()=>{
    let proc = new Promise(function(resolve, reject) {
        setTimeout(function(){
            console.log('Second Method completed...')
        } ,7000)
    });
    return proc;
}

const thirdMethod = ()=>{
    let proc = new Promise(function(resolve, reject) {
        setTimeout(function(){
            console.log('Third Method completed...')
        } ,1000)
    });
    return proc;
}


var p2 = new Promise(function(resolve, reject) {
    resolve(1);
 });
 
 p2.then(function(value) {
   console.log(value); // 1
   return new Promise(function (resolve, reject) {
     setTimeout(function () {
       resolve(value + 1);
     }, 1000);
   });
 }).then(function(value) {
   console.log(value); // 2
 });

//firstMethod().then(secondMethod()).then(thirdMethod);