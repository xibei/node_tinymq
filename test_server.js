var Server = require('./ntmq').Server;

//catch signal
process.on('SIGINT', function() {
  server.close(function() {
    console.log('end ok');
  });
});

process.on('SIGTERM', function() {
  server.close(function() {
    console.log('end ok');
  });
});

process.on('uncaughtException', function(err) {
  console.log('Caught exception:' + err);
  server.close(function() {
    console.log('end ok');
  });
});

var options = {
  'wBufSize': 128,                            //写缓存长度（M）
  'rBufSize': 128,                            //读缓存长度（M）
  'qPath': './data',                          //队列文件所在目录
  'fileMaxSize': 1024,                        //单个队列文件最大长度（M） 供参考，会超出
  'maxLength': 100000000,                     //队列中消息数上限
  'wTimeout': 30,                             //写缓存同步到文件的时间间隔（ms）
  'rTimeout': 30,                             //读缓存从文件同步的时间间隔（ms）
  'iTimeout': 100                             //记录指针文件的时间间隔（ms）
};

var server = new Server('q1', 'q2', options);   //其中q1、q2为消息队列名称，可指定任意多个消息队列

server.on('error', function(err) {
  console.log('error:' + err);
});

server.on('info', function(info) {
  console.log(info);
});

server.start(8080, '127.0.0.1', 'http', function(err) {
  if (err) {
    console.log('start faild:' + err);
    return;
  }
  console.log('start ok');
});
