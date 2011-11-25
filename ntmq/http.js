var http = require('http');
var path = require('path');

var http_wrap = {};
var buffer = new Buffer(0xffff);

http_wrap.createServer = function(callback) {
    return http.createServer(function(req, res) {
        var queue_name = getQueueName(req.url);

        //ooutput test page
        if(queue_name=='test') {
            res.writeHead(200, {'Content-Type': 'text/html; charset=UTF-8'});
            var frs = require('fs').createReadStream(path.join(path.dirname(__filename), 'test.html'));
            frs.pipe(res);
            return;
        }

        if(req.method=='GET') {
            try {
                var value = callback(false, queue_name, null);
            } catch(err) {
                res.writeHead(500, {'Content-Type': 'application/binary', 'Content-Length': err.message.length});
                res.end(err.message);
                return;
            }
            if(!value) {
                res.writeHead(200, {'Content-Type': 'application/binary', 'Content-Length': 0});
                res.end('');

            } else {
                res.writeHead(200, {'Content-Type': 'application/binary', 'Content-Length': value.length});
                res.end(value);
            }
        } else if(req.method=='POST' || req.method=='PUT') {
            var buffer_used = 0;
            var overflow = false;
            req.on('data', function(data) {
                if(overflow) {
                    return;
                }
                if(data.length>buffer.length-buffer_used) {
                    overflow = true;
                    return;
                }
                data.copy(buffer, buffer_used);
                buffer_used += data.length;
            });

            req.on('end', function() {
                if(overflow) {
                    var error = 'request entity too large';
                    res.writeHead(413, {'Content-Type': 'application/binary', 'Content-Length': error.length});
                    res.end(error);
                    return;
                }
                try {
                    callback(true, queue_name, buffer.slice(0, buffer_used));
                } catch(err) {
                    res.writeHead(500, {'Content-Type': 'application/binary', 'Content-Length': err.message.length});
                    res.end(err.message);
                    return;
                }
                res.writeHead(200, {'Content-Type': 'application/binary', 'Content-Length': 'ok'.length});
                res.end('ok');
            });
        } else {
            var error = 'only GET/POST/PUT are allowed';
            res.writeHead(405, {'Content-Type': 'application/binary', 'Content-Length': error.length});
            res.end(error);
        }
    });
};

function getQueueName(url_string) {
    var index = url_string.indexOf('/', 1);
    if(index>=0) {
        return url_string.substring(1, index);
    } else {
        index = url_string.indexOf('?', 1);
        if(index>=0) {
            return url_string.substring(1, index);
        } else {
            return url_string.substring(1);
        }
    }
}

module.exports = http_wrap;
