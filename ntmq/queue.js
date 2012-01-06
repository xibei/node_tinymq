var path = require('path');
var fs = require('fs');
var events = require('events');
var util = require('util');

//check code
var CHECK_CODE = 0x21;
var sizeBuffer = new Buffer(3);

//class Queue
function Queue(name, options) {
  events.EventEmitter.call(this);
  var self = this;

  self._name = name;                                          //队列名称
  self._qPath = path.join(options.qPath, name);               //队列文件路径

  self._wBufSize = options.wBufSize * 1024 * 1024;            //写缓冲大小（字节数）
  self._rBufSize = options.rBufSize * 1024 * 1024;            //读缓冲大小（字节数）

  self._fileMaxSize = options.fileMaxSize * 1024 * 1024;      //文件最大尺寸（字节数）
  self._maxLength = options.maxLength;                        //队列最大元素数

  self._wTimeout = options.wTimeout;                          //写入文件的间隔时间
  self._rTimeout = options.rTimeout;                          //读取文件的间隔时间
  self._iTimeout = options.iTimeout;                          //写入指针文件间隔时间

  self._init();
}

//extents EventEmitter
util.inherits(Queue, events.EventEmitter);

//initiate
Queue.prototype._init = function() {
  var self = this;

  self.length = 0;                                            //队列长度（队列内记录数）

  self._wFront = 0;                                           //写队列头部位置
  self._wRear = 0;                                            //写队列尾部位置
  self._wFileRear = 0;                                        //写队列持久化尾部位置
  self._wUsedSize = 0;                                        //写队列使用字节数
  self._wUnsavedSize = 0;                                     //写队列未持久化字节数

  self._rFront=0;                                             //读队列头部位置
  self._rRear = 0;                                            //读队列尾部位置
  self._rUsedSize = 0;                                        //读队列使用字节数

  self._rLast = 0;                                            //读队列还没有读取的字节数
  self._rLastFromWriteBuffer = 0;                             //读队列还没有读取的内容中没有在写缓冲中的字节数（_rLast-(_wUsedSize-_wUnsavedSize)）

  self._wBuffer = null;                                       //写缓冲
  self._rBuffer = null;                                       //读缓冲

  self._wFileId = 0;                                          //写文件id
  self._rFileId = 0;                                          //读文件id
  self._wFilePath = path.join(self._qPath, 'queue_' + self._wFileId);     //写文件路径
  self._rFilePath = path.join(self._qPath, 'queue_' + self._rFileId);     //读文件路径
  self._iFilePath = path.join(self._qPath, 'index');         //指针文件路径
  self._wFile = null;                                        //写文件句柄
  self._rFile = null;                                        //读文件句柄
  self._iFile = null;
  self._wFileSize = 0;                                       //已写文件大小
  self._rFileSize = 0;                                       //已读文件大小
  self._rFrontFileId = 0;                                    //读队列头部对应文件id
  self._rFrontFileOffset = 0;                                //读队列头部对应文件偏移量
  self._rFrontFileOldId = -1;                                //上一次保存的读队列头部对应文件id
  self._rFrontFileOldOffset = -1;                            //上一次保存的读队列头部对应文件偏移量

  self._wTimeoutId = null;                                   //写文件定时器id
  self._rTimeoutId = null;                                   //读文件定时器id
  self._iTimeoutId = null;                                   //指针文件定时器id

  self.status = 0;                                           //0:关闭;1:准备开始;2:开始;3:准备关闭

  self._filesList = {};                                      //已写入的文件列表，记录文件ID和长度
};

Queue.prototype._initFile = function() {
  var self = this;

  //load index file
  var indexStr;
  try {
    indexStr = fs.readFileSync(self._iFilePath, 'utf8');
  } catch (err) {}

  if (indexStr) {
    var indexes = indexStr.split(',');

    if (indexes.length >= 2) {
      var fid = parseInt(indexes[0]);
      var offset = parseInt(indexes[1]);

      if (fid >= 0 && offset >= 0) {
        self._rFrontFileId = self._rFrontFileOldId = fid;
        self._rFrontFileOffset = self._rFrontFileOldOffset = offset;

        self._rFileId = fid;
        self._rFilePath = path.join(self._qPath, 'queue_' + self._rFileId);
        self._rFileSize = offset;
      }
    }
  }

  var flags = 'w';
  if (self._rFrontFileOldId < 0) {
    self.emit('info', 'new queue');
    return flags;
  }

  self.emit('info', 'file_id:' + self._rFrontFileOldId+' offset:' + self._rFrontFileOldOffset);
  //set flag to r+
  flags = 'r+';

  //get files' rear
  var fd = fs.openSync(self._rFilePath, 'r', 0755);     //当前文件描述符
  var fid = self._rFileId;                               //当前文件id
  var offset = self._rFileSize;                                  //当前读取文件偏移量
  var buffer = new Buffer(102400);

  var lastFileId = fid;
  var lastFileSize = offset;
  var maybeLastFileId = lastFileId;
  var maybeLastFileSize = lastFileSize;

  var broken = false;
  var expect = 0;     //0:check code;1:size;2:value
  var size = 0;
  var sizeLast = 0;
  var valueLast = 0;

  while (true) {
    try {
      var bytesRead = fs.readSync(fd, buffer, 0, buffer.length, offset);
    } catch (err) {
      fs.close(fd);
      throw err;
    }

    for (var i = 0; i < bytesRead; i++) {
      if (expect == 0) {
        if (buffer[i] != CHECK_CODE) {
          broken = true;
          fs.close(fd);
          break;
        }
        if (size > 0) {
          self.length++;
          self._rLast += 3 + size;
          self._rLastFromWriteBuffer += 3 + size;
          lastFileId = maybeLastFileId;
          lastFileSize = maybeLastFileSize;

          if (self.length >= self._maxLength) {
            broken = true;
            fs.close(fd);
            break;
          }
        }
        sizeLast = 2;
        expect = 1;
      } else if (expect == 1) {
        if (bytesRead - i >= sizeLast) {
          buffer.copy(sizeBuffer, 2-sizeLast, i, i+sizeLast);
          size = sizeBuffer.readUInt16BE(0);
          if (size <= 0) {
            broken = true;
            fs.close(fd);
            break;
          }
          i += sizeLast - 1;
          valueLast = size;
          expect = 2;
        } else {
          buffer.copy(sizeBuffer, 2 - sizeLast, i, bytesRead);
          sizeLast -= bytesRead - i;
          i += bytesRead - i - 1;
        }
      } else {
        if (bytesRead - i >= valueLast) {
          i += valueLast - 1;
          maybeLastFileId = fid;
          maybeLastFileSize = offset + i + 1;
          expect = 0;
        } else {
          valueLast -= bytesRead - i;
          i += bytesRead - i - 1;
        }
      }
    }
    if (broken) {
      break;
    }

    offset += bytesRead;

    // open new file
    if (bytesRead < buffer.length) {
      fs.close(fd);

      var nextPath = path.join(self._qPath, 'queue_' + (fid + 1));
      if (!path.existsSync(nextPath)) {
        if (expect == 0) {     //normal file end
          if (size > 0) {
            self.length++;
            self._rLast += 3 + size;
            self._rLastFromWriteBuffer += 3 + size;
            lastFileId = maybeLastFileId;
            lastFileSize = maybeLastFileSize;
          }
        } else {
          broken = true;
        }
        break;
      }

      self._filesList[fid] = offset;
      fd = fs.openSync(nextPath, 'r', 0755);
      fid++;
      offset = 0;
    }
  }
  fs.close(fd);


  self._wFileId = lastFileId;
  self._wFilePath = path.join(self._qPath, 'queue_' + self._wFileId);
  self._wFileSize = lastFileSize;

  // clear excess fid
  var excessFileId = self._wFileId + 1;
  while (typeof self._filesList[excessFileId] != 'undefined') {
    delete self._filesList[excessFileId];
    excessFileId++;
  }

  //truncate file
  if (broken) {
    var lastFile = fs.openSync(self._wFilePath, 'r+', 0755);
    try {
      fs.truncateSync(lastFile, self._wFileSize);
    } catch (err) {
      fs.close(lastFile);
      throw err;
    }
  }

  self.emit('info', 'last_fid:' + self._wFileId + ' last_fsize:' + self._wFileSize + ' queue_length:' + self.length + ' file_bytes:' + self._rLast + ' broken:' + broken);
  return flags;
};

//start queue service
Queue.prototype.start = function(callback) {
  var self = this;

  if (self.status != 0) {
    var err = new Error('queue\'s status is not stopped');
    self.emit('error', err);
    callback && callback.call(self, err);
    return;
  }

  self._init();
  self.status = 1;

  //mkdir
  var exists = path.existsSync(self._qPath);
  if (!exists) {
    try {
      fs.mkdirSync(self._qPath, 0755);
    } catch (err) {
      self.status = 0;
      self.emit('error', err);
      callback && callback.call(self, err);
      return;
    }
  }

  try {
    var flags = self._initFile();
  } catch (err) {
    self.status = 0;
    self.emit('error', err);
    callback && callback.call(self, err);
    return;
  }

  //open file
  //open index file
  try {
    self._iFile = fs.openSync(self._iFilePath, flags, 0775);
  } catch (err) {
    self.status = 0;
    self.emit('error', err);
    callback && callback.call(self, err);
    return;
  }

  try {
    self._wFile = fs.openSync(self._wFilePath, flags, 0755);
  } catch (err) {
    self._iFile = null;

    self.status = 0;
    self.emit('error', err);
    callback && callback.call(self, err);
    return;
  }

  try {
    self._rFile = fs.openSync(self._rFilePath, 'r', 0755);
  } catch (err) {
    self._iFile = null;
    self._wFile = null;

    self.status = 0;
    self.emit('error', err);
    callback && callback.call(self, err);
    return;
  }

  //allocate buffer memory
  self._wBuffer = new Buffer(self._wBufSize);
  self._rBuffer = new Buffer(self._rBufSize);

  self._wTimeoutId = setTimeout(function() {
    self._save();
  }, self._wTimeout);

  self._rTimeoutId = setTimeout(function() {
    self._load();
  }, self._rTimeout);

  self._iTimeoutId = setTimeout(function() {
    self._index();
  }, 1);

  //delete next file
  var nextPath = path.join(self._qPath, 'queue_' + (self._wFileId + 1));
  fs.unlink(nextPath);

  self.status = 2;
  self.emit('start');
  callback && callback.call(self);
};

//write queue
Queue.prototype._save = function() {
  var self = this;

  //nothing to save
  if (self._wUnsavedSize <= 0) {
    //stopping
    if (self.status == 3) {
      fs.close(self._wFile);
      self._wFile = null;
      self._wBuffer = null;
      self._wTimeoutId = null;
      return;
    }

    self._wTimeoutId = setTimeout(function() {
      self._save();
    }, self._wTimeout);
    return;
  }

  var fileFreeSize = self._fileMaxSize - self._wFileSize;
  //current file is full
  if (fileFreeSize <= 0) {
    //save current file length
    self._filesList[self._wFileId] = self._wFileSize;

    //close current file
    fs.close(self._wFile);

    //open new file
    self._wFileId++;
    self._wFilePath = path.join(self._qPath, 'queue_' + self._wFileId);
    self._wFileSize = 0;

    fs.open(self._wFilePath, 'w', 0755, function(err, fd) {
      if (err) {
        self._wFile = null;
        self._wBuffer = null;
        self._wTimeoutId = null;

        self.emit('error', err);
        self.stop();
        return;
      }
      self._wFile = fd;
      self._save();
    });

    //delete next file
    var nextPath = path.join(self._qPath, 'queue_'+ (self._wFileId + 1));
    fs.unlink(nextPath);

    return;
  }

  //save
  var saveSize;
  //loop
  if (self._wBufSize - self._wFileRear < self._wUnsavedSize) {
    saveSize = self._wBufSize - self._wFileRear;
  } else {
    saveSize = self._wUnsavedSize;
  }
  saveSize = saveSize < fileFreeSize ? saveSize : fileFreeSize;
  fs.write(self._wFile, self._wBuffer, self._wFileRear, saveSize, self._wFileSize, function(err, written, buffer) {
    if (err) {
      fs.close(self._wFile);
      self._wFile = null;
      self._wBuffer = null;
      self._wTimeoutId = null;

      self.emit('error', err);
      self.stop();
      return;
    }

    self._wUnsavedSize -= written;
    self._wFileSize += written;
    self._wFileRear += written;
    if (self._wFileRear >= self._wBufSize) {
      self._wFileRear -= self._wBufSize;
    }
    self._rLast += written;
    self._wTimeoutId = setTimeout(function() {
      self._save();
    }, 1);
  });
};

//enQueue
Queue.prototype.enQueue = function(value) {
  var self = this;

  var type;
  if (Buffer.isBuffer(value)) {
    type = 'buffer';
  } else if(util.isArray(value)) {
    type = 'array';
  } else {
    throw new TypeError('param type error');
  }

  if (type == 'buffer' && value.length <= 0) {
    throw new Error('value is empty');
  }
  if (type == 'array') {
    value.size = 0;
    for (var i = 0; i < value.length; i++) {
      value.size += value[i].length;
    }
    if (value.size <= 0) {
      throw new Error('value is empty');
    }
  }

  if (self.status != 2) {
    throw new Error('queue is not started');
  }

  var size = (type == 'buffer') ? value.length : value.size;
  if (size > 0xFFFF) {
    throw new Error('value length is too big(don\'t exceed 65535 bytes)');
  }
  if (size + 3 > self._wBufSize - self._wUnsavedSize) {
    throw new Error('write buffer is full');
  }
  if (self.length >= self._maxLength) {
    throw new Error('queue is full');
  }

  sizeBuffer.writeUInt8(CHECK_CODE, 0);
  sizeBuffer.writeUInt16BE(size, 1);
  var rear;

  if (self._wBufSize >= self._wRear + 3) {
    rear = self._wRear + 3;
    sizeBuffer.copy(self._wBuffer, self._wRear, 0, 3);
  } else {
    rear = 3 - (self._wBufSize - self._wRear);
    sizeBuffer.copy(self._wBuffer, self._wRear, 0, 3 - rear);
    sizeBuffer.copy(self._wBuffer, 0, 3 - rear, 3);
  }

  if (self._wBufSize >= rear + size) {
    self._wRear = rear + size;

    if (type == 'buffer') {
      value.copy(self._wBuffer, rear, 0, size);
    } else {
      var targetStart = rear;
      for (var i = 0; i < value.length; i++) {
        value[i].copy(self._wBuffer, targetStart, 0, value[i].length);
        targetStart += value[i].length;
      }
    }
  } else {
    self._wRear = size - (self._wBufSize - rear);

    if (type == 'buffer') {
      value.copy(self._wBuffer, rear, 0, size - self._wRear);
      value.copy(self._wBuffer, 0, size - self._wRear, size);
    } else {
      var targetStart = rear;
      for (var i = 0; i < value.length; i++) {
        if (targetStart + value[i].length <= self._wBufSize) {
          value[i].copy(self._wBuffer, targetStart, 0, value[i].length);
          targetStart += value[i].length;
        } else {
          value[i].copy(self._wBuffer, targetStart, 0, self._wBufSize - targetStart);
          var sourceStart = self._wBufSize - targetStart;
          break;
        }
      }
      targetStart = 0;
      for (; i < value.length; i++) {
        value[i].copy(self._wBuffer, targetStart, sourceStart, value[i].length);
        targetStart += value[i].length - sourceStart;
        sourceStart = 0;
      }
    }
  }

  if (self._wRear >= self._wBufSize) {
    self._wRear -= self._wBufSize;
  }

  self.length++;
  self._wUnsavedSize += 3 + size;
  self._wUsedSize += 3 + size;
  if (self._wUsedSize > self._wBufSize) {
    self._wFront += self._wUsedSize - self._wBufSize;
    if (self._wFront >= self._wBufSize) {
      self._wFront -= self._wBufSize;
    }
    self._rLastFromWriteBuffer += self._wUsedSize - self._wBufSize;
    self._wUsedSize = self._wBufSize;
  }
};

//read queue
Queue.prototype._load = function() {
  var self = this;

  //stopping
  if (self.status == 3) {
    fs.close(self._rFile);
    self._rFile = null;
    self._rBuffer = null;
    self._rTimeoutId = null;
    return;
  }

  var loadSize = self._rBufSize - self._rUsedSize;
  loadSize = loadSize < (self._rBufSize - self._rRear) ? loadSize : (self._rBufSize - self._rRear);
  loadSize = loadSize < self._rLast ? loadSize : self._rLast;

  //_rFile full or empty file
  if (loadSize <= 0) {
    self._rTimeoutId = setTimeout(function() {
      self._load();
    }, self._rTimeout);
    return;
  }

  //load
  fs.read(self._rFile, self._rBuffer, self._rRear, loadSize, self._rFileSize, function(err, bytesRead, buffer) {
    if (err) {
      fs.close(self._rFile);
      self._rFile = null;
      self._rBuffer = null;
      self._rTimeoutId = null;

      self.emit('error', err);
      self.stop();
      return;
    }

    self._rRear += bytesRead;
    if (self._rRear >= self._rBufSize) {
      self._rRear -= self._rBufSize;
    }
    self._rUsedSize += bytesRead;
    self._rLast -= bytesRead;
    self._rLastFromWriteBuffer -= bytesRead;
    self._rFileSize += bytesRead;

    //file loop
    if (bytesRead < loadSize) {
      //close current file
      fs.close(self._rFile);

      //open new file
      self._rFileId++;
      self._rFilePath = path.join(self._qPath, 'queue_' + self._rFileId);
      self._rFileSize = 0;

      fs.open(self._rFilePath, 'r', 0755, function(err, fd) {
        if (err) {
          self._rFile = null;
          self._rBuffer = null;
          self._rTimeoutId = null;

          self.emit('error', err);
          self.stop();
          return;
        }
        self._rFile = fd;
        self._load();
      });
      return;
    }

    self._rTimeoutId = setTimeout(function() {
      self._load();
    }, 1);
  });
};

//deQueue
Queue.prototype.deQueue = function() {
  var self = this;

  if (self.length <= 0) {
    return null;
  }
  if (self.status != 2) {
    throw new Error('queue is not started');
  }

  if (self._rUsedSize < 3) {
    throw new Error('read buffer is empty');
  }
  var front;
  if (self._rBufSize >= self._rFront + 3) {
    front = self._rFront + 3;
    self._rBuffer.copy(sizeBuffer, 0, self._rFront, front);
  } else {
    front = 3 - (self._rBufSize - self._rFront);
    self._rBuffer.copy(sizeBuffer, 0, self._rFront, self._rBufSize);
    self._rBuffer.copy(sizeBuffer, 3 - front, 0, front);
  }
  //check code
  if (sizeBuffer.readUInt8(0) != CHECK_CODE) {
    throw new Error('check code error');
  }
  var size = sizeBuffer.readUInt16BE(1);

  if (self._rUsedSize < 3 + size) {
    throw new Error('read buffer is empty');
  }
  var value = new Buffer(size);
  if (self._rBufSize >= front + size) {
    self._rFront = front + size;
    self._rBuffer.copy(value, 0, front, self._rFront);
  } else {
    self._rFront = size - (self._rBufSize - front);
    self._rBuffer.copy(value, 0, front, self._rBufSize);
    self._rBuffer.copy(value, size - self._rFront, 0, self._rFront);
  }
  if (self._rFront >= self._rBufSize) {
    self._rFront -= self._rBufSize;
  }
  self.length--;
  self._rUsedSize -= 3 + size;

  self._rFrontFileOffset += 3 + size;
  while (self._filesList[self._rFrontFileId] < self._rFrontFileOffset) {
    self._rFrontFileOffset -= self._filesList[self._rFrontFileId];
    self._rFrontFileId++;
  }
  return value;
};

//index
Queue.prototype._index = function() {
  var self = this;

  //nothing to save
  if (self._rFrontFileOldId == self._rFrontFileId && self._rFrontFileOldOffset == self._rFrontFileOffset) {
    //stopping
    if (self.status == 3) {
      fs.close(self._iFile);
      self._iFile = null;
      self._iTimeoutId = null;
      return;
    }

    self._iTimeoutId = setTimeout(function() {
      self._index();
    }, self._iTimeout);
    return;
  }

  self._rFrontFileOldId = self._rFrontFileId;
  self._rFrontFileOldOffset = self._rFrontFileOffset;

  var indexStr = self._rFrontFileId + ',' + self._rFrontFileOffset + ',';
  fs.write(self._iFile, new Buffer(indexStr), 0, indexStr.length, 0, function(err, written, buffer) {
    if (err) {
      fs.close(self._iFile);
      self._iFile = null;
      self._iTimeoutId = null;

      self.emit('error', err);
      self.stop();
      return;
    }
    fs.truncate(self._iFile, indexStr.length, function(err) {
      if (err) {
        fs.close(self._iFile);
        self._iFile = null;
        self._iTimeoutId = null;

        self.emit('error', err);
        self.stop();
        return;
      }
      //delete old file
      for (var i = self._rFrontFileOldId - 1; i >= 0 && typeof self._filesList[i] != 'undefined'; i--) {
        var oldPath = path.join(self._qPath, 'queue_' + i);
        fs.unlink(oldPath);
        delete self._filesList[i];
      }
      self._iTimeoutId = setTimeout(function() {
        self._index();
      }, self._iTimeout);
    });
  });
};

//stop queue service
Queue.prototype.stop = function(callback) {
  var self = this;

  //closed or closing
  if (self.status == 0 || self.status == 3) {
    callback && callback.call(self);
    return;
  }
  //starting
  if (self.status == 1) {
    self.once('start', function() {
      self.stop(callback);
    });
    return;
  }

  self.status = 3;

  if (!self._wTimeoutId) {
    if (self._wFile) {
      fs.close(self._wFile);
      self._wFile = null;
    }
    self._wBuffer = null;
  }
  if (!self._rTimeoutId) {
    if (self._rFile) {
      fs.close(self._rFile);
      self._rFile = null;
    }
    self._rBuffer = null;
  }
  if (!self._iTimeoutId) {
    if (self._iFile) {
      fs.close(self._iFile);
      self._iFile = null;
    }
  }

  var waitingStop = function () {
    if (self.status != 3) {
      callback && callback.call(self);
      return;
    }
    if (!self._wFile && !self._wBuffer && !self._wTimeoutId && !self._rFile && !self._rBuffer && !self._rTimeoutId && !self._iFile && !self._iTimeoutId) {
      self.status = 0;
      self.emit('stop');
      callback && callback.call(self);
      return;
    }
    setTimeout(waitingStop, 10);
  };
  setTimeout(waitingStop, 10);
};

//exports
module.exports = Queue;
