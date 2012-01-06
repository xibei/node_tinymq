var http = require('http');
var path = require('path');

var httpWrap = {};

httpWrap.createServer = function(callback) {
  return http.createServer(function(req, res) {
    var queueName = getQueueName(req.url);

    //output test page
    if (queueName == 'test') {
      res.writeHead(200, {'Content-Type': 'text/html; charset=UTF-8'});
      var frs = require('fs').createReadStream(path.join(path.dirname(__filename), 'test.html'));
      frs.pipe(res);
      return;
    }

    if (req.method == 'GET') {
      try {
        var value = callback(false, queueName, null);
      } catch (err) {
        res.writeHead(500, {'Content-Type': 'application/binary', 'Content-Length': err.message.length});
        res.end(err.message);
        return;
      }
      if (!value) {
        res.writeHead(200, {'Content-Type': 'application/binary', 'Content-Length': 0});
        res.end('');
      } else {
        res.writeHead(200, {'Content-Type': 'application/binary', 'Content-Length': value.length});
        res.end(value);
      }
    } else if (req.method == 'POST' || req.method == 'PUT') {
      var dataArray = [];
      req.on('data', function(data) {
        dataArray.push(data);
      });

      req.on('end', function() {
        try {
          callback(true, queueName, dataArray);
        } catch (err) {
          res.writeHead(500, {'Content-Type': 'application/binary', 'Content-Length': err.message.length});
          res.end(err.message);
          return;
        }
        res.writeHead(200, {'Content-Type': 'application/binary', 'Content-Length': 2});
        res.end('ok');
        /*
        res.connection.write('HTTP/1.1 200 OK\r\nContent-Type: application/binary\r\nContent-Length: ' + 2 + '\r\n\r\nok');
        res._headerSent = true;
        res.finished = true;
        if (res.output.length === 0 && res.connection._httpMessage === res) {
          res._finish();
        }
        */
      });
    } else {
      var error = 'only GET/POST/PUT are allowed';
      res.writeHead(405, {'Content-Type': 'application/binary', 'Content-Length': error.length});
      res.end(error);
    }
  });
};

function getQueueName(urlString) {
  var index = urlString.indexOf('/', 1);
  if (index >= 0) {
    return urlString.substring(1, index);
  } else {
    index = urlString.indexOf('?', 1);
    if (index >= 0) {
      return urlString.substring(1, index);
    } else {
      return urlString.substring(1);
    }
  }
}

module.exports = httpWrap;
