var events = require('events');
var util = require('util');
var Queue = require('./queue');

function Server() {
  var self = this;

  //get queue's name
  var args = [].slice.call(arguments, 0);
  self._options = args[args.length - 1];
  self._queueNames = args.slice(0, args.length - 1);

  self._queues = {};
  for (var i = 0; i < self._queueNames.length; i++) {
    self._queues[self._queueNames[i]] = new Queue(self._queueNames[i], self._options);
    (function() {
       var queueName = self._queueNames[i];
       self._queues[queueName].on('info', function(info) {
         self.emit('info', 'queue ' + queueName + ' info: ' + info);
       });
    })();
  }

  self._protocolServer = null;
}

//extend EventEmitter
util.inherits(Server, events.EventEmitter);

//stop all queues
Server.prototype._stopQueues = function(callback) {
  var self = this;

  //stop all queues
  var stoppedNum = 0;
  for (var q in self._queues) {
    self._queues[q].stop(function() {
      stoppedNum++;
      if (stoppedNum == self._queueNames.length) {
        callback && callback.call(self);
      }
    });
  }
};

//start
Server.prototype.start = function(port, hostname, protocol, callback) {
  var self = this;

  self._port = port;

  if (!protocol) {
    protocol = 'http';
  }

  //start queue
  var startedNum = 0;
  var startedOk = true;
  for (var q in self._queues) {
    self._queues[q].start(function(err) {
      startedNum++;
      if (err) {
        startedOk = false;
      }
      if (startedNum < self._queueNames.length) {
        return;
      }

      //failed
      if (!startedOk) {
        self._stopQueues(function() {
          var err = new Error('queues start faild');
          self.emit('error', err);
          callback && callback.call(self, err);
        });
        return;
      }

      //start protocol server
      self._protocolServer = require('./' + protocol).createServer(function(enQueue, queueName, value) {
        var q = self._queues[queueName];
        if (!q) {
          throw new Error('queue is not exist');
        }
        if (enQueue) {
          q.enQueue(value);
        } else {
          return q.deQueue();
        }
      });
      self._protocolServer.listen(port, hostname, function() {
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
  if (self._protocolServer) {
    self._protocolServer.close();
    self._protocolServer = null;
  }

  //stop queues
  self._stopQueues(function() {
    self.emit('close');
    callback && callback.call(self);
  });
};

//exports
module.exports = Server;
