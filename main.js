var kafka = require('kafka-node');

// PRODUCER CONFI
const client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
var HighLevelProducer = kafka.HighLevelProducer,
    producer = new HighLevelProducer(client);

// CONSUMER CONFI    
var Consumer = kafka.Consumer;
const client1 = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
var consumer = new Consumer(
        client1,
        [
            { topic: 'nodehack7', partition: 0 }],
        {
            autoCommit: false
        }
    );

// FILE CONFI
var fs = require('fs');
var readline = require('readline');
var stream = require('stream');

var instream = fs.createReadStream('1mb.txt');
var outstream = new stream;
var rl = readline.createInterface(instream, outstream);



var start= new Date();

rl.on('line', function(line) {

  var payloads = [
        { topic: 'nodehack7', messages: line}
    ];
    
producer.on('ready', function () {

    
    producer.send(payloads, function (err, data) {
        //console.log(data);
        //console.log("--------------------------------------")
        
    });
});

//console.log('***************** CONSUMER *************************');
consumer.on('message', function (message) {
  //  console.log(message);
    //console.log('***************************************');
    
});
});
var end= new Date();

rl.on('close', function() {
    var end= new Date();
  console.log("time require to send data from Producer to Consumer: "+(end-start)/1000+" sec");
  process.exit(1);
});












/*var kafka = require('kafka-node');
var fs=require('fs');
const client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
var HighLevelProducer = kafka.HighLevelProducer,
    //client = new kafka.Client(),
    producer = new HighLevelProducer(client);
    
/*
if (process.argv.length < 3) {
  console.log('Usage: node ' + process.argv[1] + ' 1mb.txt');
  process.exit(1);
}
// Read the file and print its contents.
var fs = require('fs')
  , filename = process.argv[2];
fs.readFile(filename, 'utf8', function(err, data) {
  if (err) throw err;
  let d=data;
});









var data= fs.readFileSync('1mb.txt', 'utf8');
console.log(data);


   var payloads = [
        { topic: 'nodehack', messages: data}
        
    ];
    console.log('***************** PRODUCER *************************');
    var start= new Date();
producer.on('ready', function () {

    
    producer.send(payloads, function (err, data) {
        console.log(data);
        console.log('***************************************');
    });
});

var Consumer = kafka.Consumer;
const client1 = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
var consumer = new Consumer(
        client1,
        [
            { topic: 'nodehack', partition: 0 }],
        {
            autoCommit: false
        }
    );
console.log('***************** CONSUMER *************************');
consumer.on('message', function (message) {
    console.log(message);
    console.log('***************************************');
    
});
var end= new Date();
    console.log("time require to send data from Producer to Consumer: "+(end-start)/1000);

*/
