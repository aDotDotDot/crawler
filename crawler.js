const request = require('request').defaults({
  headers: {'user-agent': 'i am a nice little crawler'}
});
const DOMParser = require('xmldom').DOMParser;
const moment = require('moment');
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;//may fail on a VPS
const amqp = require('amqplib/callback_api');

const readline = require('readline');


const DOMParserOptions = {
  /**
   * locator is always need for error position info
   */
  locator:{},
  /**
   * you can override the errorHandler for xml parser
   * @link http://www.saxproject.org/apidoc/org/xml/sax/ErrorHandler.html
   */
  errorHandler:{warning:()=>{},error:()=>{},fatalError:()=>{}}
  //errorHandler:function(level,msg){console.log(level,msg)}
};
  
let urlStartTest , wordSearchTest;

const args = process.argv.slice(2);
if(args.length<2){
  console.warn('Usage : node crawler.js <start url> <word/regexp>')
  process.exit(1);
}else{
  try{
    let parsedUrl = new URL(args[0]);
    urlStartTest = parsedUrl.href;
    wordSearchTest = new RegExp(args[1],'i')
  }catch(e){
    console.warn('Usage : node crawler.js <start url> <word/regexp>')
    console.warn('Please check the validity of the URL')
    process.exit(1);
  }
}

//returns the current url, eventual matches, or a list of links to crawl next if nothing found
const getUrlLinksAndMatch = (url, match, depth=0) => {
    return new Promise( (resolve, reject)=>{
      request(url, async (error, res, body)=>{
        if(error)
          reject(error);
        //console.log(`Requesting ${url}`);
        const requestedURL = new URL(url);
        const local = requestedURL.host;
        const doc = new DOMParser(DOMParserOptions).parseFromString(body, 'text/html');
        const pageBody = doc.getElementsByTagName('body')[0];
        if(match.test(pageBody.textContent)){//found in the body
          let where = pageBody.textContent.search(match);
          resolve({found:true, search:match, url:url, where: where, textMatching: pageBody.textContent.substring(where-100, where+100), links:{local:null, ext:null}, depth:depth});
        }else{
          const allPageLinks = pageBody.getElementsByTagName('a');
          const localLinks = new Set();
          const extLinks = new Set();
          for(let i = 0; i < allPageLinks.length; i++){
            let aUrl = new URL(allPageLinks.item(i).getAttribute('href'), url);
            if(!(aUrl.host == local && aUrl.pathname == '/')){
              if(aUrl.host == local)
                localLinks.add(aUrl.pathname);
              else
                extLinks.add(aUrl.href);
            }
          }
          resolve({found:false, search:match, url:url, where:null, links:{local:localLinks, ext:extLinks, base:requestedURL.origin}, depth:depth});
        }
    });
  });
};



const doSearchAndAdd = (q, ch, url, search)=>{
  return new Promise( (resolve, reject)=>{
    getUrlLinksAndMatch(url, search).then(what=>{
      if(what.found){
        process.send({found:true, message:`Found in ${what.url}, at position ${what.where}, something like: \n${what.textMatching}`});        resolve('found');
      }else{
        let nextLinks = [...what.links.local];
        let baseURL = what.links.base;
        nextLinks.map(e=>{
          let fUrl = new URL(e, baseURL);
          ch.sendToQueue(q, new Buffer.from(fUrl.href));
        });
        resolve('nope');
      }
    }).catch(err=>{reject(err)});
  });
};
let locked = false;
const clearLines = (msgs)=>{
  readline.moveCursor(process.stdout,0,-msgs.size);
  readline.clearScreenDown(process.stdout);
}
const updateProgress = (msgs)=>{
  if(locked)
    return;
  clearLines(msgs);
  let m = [...msgs];
  for(let i=0;i<msgs.size;i++){
    console.log(m[i][1]);
  }
};
//console.log(cluster);
if (cluster.isMaster) {
  //console.log(`Master ${process.pid} is running`);
  getUrlLinksAndMatch(urlStartTest, wordSearchTest).then(what=>{
    if(what.found){
      console.log(`Found in ${what.url}, at position ${what.where}, something like: \n${what.textMatching}`);
    }else{
      //console.log(what.links, [...what.links.local]);
      console.log(`Beginning search in ${what.url}, using ${numCPUs} thread${numCPUs>1?'s':''}`);
      for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
        console.log(`Starting new thread...`);
      }
      let msgs = new Map();
      amqp.connect('amqp://localhost', function(err, conn) {
        conn.createChannel(function(err, ch) {
          const q = 'crawling_queue';
          ch.assertQueue(q, {durable: false});
          let nextLinks = [...what.links.local];
          let baseURL = what.links.base;
          nextLinks.map(e=>{
            let fUrl = new URL(e, baseURL);
            ch.sendToQueue(q, new Buffer.from(fUrl.href));
          });
        });
      });

      let nbDead = 0;
      cluster.on('exit', (worker, code, signal) => {
        //console.log(`worker ${worker.process.pid} died`);
        nbDead++;
        if(nbDead>=numCPUs)
          process.exit(0);
      });
      cluster.on('error', ()=>{});
      cluster.on('message', (worker, msg)=>{
        //console.log('onmsg',msg);
        if(msg.consuming){
          msgs.set(worker.process.pid,'Searching in '+msg.search);
          updateProgress(msgs)
        }
        if(msg.found){
          locked=true;
          for (const id in cluster.workers) {
            cluster.workers[id].kill();
          }
          clearLines(msgs);
          console.log(msg.message);
        }

      });
    }
  }).catch(console.warn);
  // Fork workers.
} else {
  // Workers can share any TCP connection
  // In this case it is an HTTP server
  //console.log(`Worker ${process.pid} started`);
  process.send({starting:true,pid:process.pid});
  amqp.connect('amqp://localhost', function(err, conn) {
    conn.createChannel(function(err, ch) {
      const q = 'crawling_queue';
      ch.assertQueue(q, {durable: false});
      //console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", q);
      ch.prefetch(1);
      ch.consume(q, function(msg) {
        //console.log(`Worker ${process.pid} [x] Received ${msg.content.toString()}`);
        process.send({found:false, consuming:true, search:msg.content.toString()});
        doSearchAndAdd(q, ch, msg.content.toString(), wordSearchTest).then(e=>{
          ch.ack(msg);
          if(e=='found'){
            ch.purgeQueue(q);
            //process.send({found:true});
          }
        }).catch(err=>{
          ch.ack(msg);
          console.warn(err);
        })
      }, {noAck: false});
    });
  });
}

