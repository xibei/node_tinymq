var events = require('events');
var Queue = require('./queue');

function Server() {
    var self = this;

    //get queue's name
    var args = [].slice.call(arguments, 0);
    self.options = args[args.length-1];
    self.q_name = args.slice(0, args.length-1);

    self.queues = {};
    for(var i=0; i<self.q_name.length; i++) {
        self.queues[self.q_name[i]] = new Queue(self.q_name[i], self.options);
        (function() {
            var q_name = self.q_name[i];
            self.queues[q_name].on('info', function(info) {
                self.emit('info', 'queue '+q_name+' info: '+info);
            });
        })();
    }

    self.p_server = null;
}

//extend EventEmitter
Server.prototype = new events.EventEmitter();

//stop all queues
Server.prototype.stop_queues = function(callback) {
    var self = this;

    //stop all queues
    var stopped_num = 0;
    for(var q in self.queues) {
        self.queues[q].stop(function() {
            stopped_num++;
            if(stopped_num==self.q_name.length) {
                callback && callback.call(self);
            }
        });
    }
};

//start
Server.prototype.start = function(port, hostname, protocol, callback) {
    var self = this;

    self.port = port;

    if(!protocol) {
        protocol = 'http';
    }
    
    //start queue
    var started_num = 0;
    var started_ok = true;
    for(var q in self.queues) {
        self.queues[q].start(function(err) {
            started_num++;
            if(err) {
                started_ok = false;
            }
            if(started_num<self.q_name.length) {
                return;
            }

            //faild
            if(!started_ok) {
                self.stop_queues(function() {
                    var err = new Error('queues start faild');
                    self.emit('error', err);
                    callback && callback.call(self, err);
                });
                return;
            }

            //start protocol server
            self.p_server = require('./'+protocol).createServer(function(en_queue, q_name, value){
                var q = self.queues[q_name];
                if(!q) {
                    throw new Error('queue is not exist');
                }
                if(en_queue) {
                    q.enQueue(value);
                } else {
                    return q.deQueue();
                }
            });
            self.p_server.listen(port, hostname, function() {
                self.emit('start');
                callback && callback.call(self);
            });
        });
    }
};

//close
Server.prototype.close = function(callback) {
    var self = this;

    //stop protocol server
    if(self.p_server) {
        self.p_server.close();
        self.p_server = null;
    }

    //stop queues
    self.stop_queues(function() {
        self.emit('close');
        callback && callback.call(self);
    });
};

//exports
module.exports = Server;
