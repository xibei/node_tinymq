// load config
var path = require('path');
var fs = require('fs');
var events = require('events');

//check code
var check_code = 0x21;

//class Queue
function Queue(name, options) {
    var self = this;

    self.name = name;                                       //队列名称
    self.q_path = path.join(options.q_path, name);           //队列文件路径

    self.w_buf_size = options.w_buf_size * 1024 * 1024;      //写缓冲大小（字节数）
    self.r_buf_size = options.r_buf_size * 1024 * 1024;      //读缓冲大小（字节数）

    self.file_max_size = options.file_max_size * 1024 * 1024;    //文件最大尺寸（字节数）
    self.max_length = options.max_length;                    //队列最大元素数

    self.w_timeout = options.w_timeout;                      //写入文件的间隔时间
    self.r_timeout = options.r_timeout;                      //读取文件的间隔时间
    self.i_timeout = options.i_timeout;                      //写入指针文件间隔时间

    self.init();
}

//extents EventEmitter
Queue.prototype = new events.EventEmitter();

//init
Queue.prototype.init = function() {
    var self = this;

    self.length = 0;                                        //队列长度（队列内记录数）

    self.w_front = 0;                                       //写队列头部位置
    self.w_rear = 0;                                        //写队列尾部位置
    self.wf_rear = 0;                                       //写队列持久化尾部位置
    self.w_used_size = 0;                                   //写队列使用字节数
    self.w_unsaved_size = 0;                                //写队列未持久化字节数

    self.r_front=0;                                         //读队列头部位置
    self.r_rear = 0;                                        //读队列尾部位置
    self.r_used_size = 0;                                   //读队列使用字节数

    self.r_last = 0;                                        //读队列还没有读取的字节数
    self.r_w_last = 0;                                      //读队列还没有读取的内容中没有被写缓冲的字节数（r_last-(w_used_size-w_unsaved_size)）

    self.w_buffer = null;                                   //写缓冲
    self.r_buffer = null;                                   //读缓冲

    self.w_file_id = 0;                                     //写文件id
    self.r_file_id = 0;                                     //读文件id
    self.w_file_path = path.join(self.q_path, 'queue_'+self.w_file_id);     //写文件路径
    self.r_file_path = path.join(self.q_path, 'queue_'+self.r_file_id);     //读文件路径
    self.i_file_path = path.join(self.q_path, 'index');     //指针文件路径
    self.w_file = null;                                     //写文件句柄
    self.r_file = null;                                     //读文件句柄
    self.i_file = null;
    self.w_file_size = 0;                                   //已写文件大小
    self.r_file_size = 0;                                   //已读文件大小
    self.r_front_file_id = 0;                               //读队列头部对应文件id
    self.r_front_file_offset = 0;                           //读队列头部对应文件偏移量
    self.r_front_file_id_old = -1;                          //上一次保存的读队列头部对应文件id
    self.r_front_file_offset_old = -1;                      //上一次保存的读队列头部对应文件偏移量

    self.w_timeout_id = null;                               //写文件定时器id
    self.r_timeout_id = null;                               //读文件定时器id
    self.i_timeout_id = null;                               //指针文件定时器id

    self.status = 0;                                        //0:关闭;1:准备开始;2:开始;3:准备关闭

    self.f_list = {};                                       //已写入的文件列表，记录文件ID和长度
};

Queue.prototype.init_file = function() {
    var self = this;

    //load index file
    var index_str;
    try {
        index_str = fs.readFileSync(self.i_file_path, 'utf8');
    } catch(err) {}

    if(index_str) {
        var indexes = index_str.split(',');
        if(indexes.length>=2) {
            var fid = parseInt(indexes[0]);
            var offset = parseInt(indexes[1]);
            if(fid>=0 && offset>=0) {
                self.r_front_file_id = self.r_front_file_id_old = fid;
                self.r_front_file_offset = self.r_front_file_offset_old = offset;

                self.r_file_id = fid;
                self.r_file_path = path.join(self.q_path, 'queue_'+self.r_file_id);
                self.r_file_size = offset;
            }
        }
    }

    var flags = 'w';
    if(self.r_front_file_id_old<0) {
        self.emit('info', 'new queue');
        return flags;
    }

    self.emit('info', 'file_id:'+self.r_front_file_id_old+' offset:'+self.r_front_file_offset_old);
    //set flag to r+
    flags = 'r+';

    //get files's rear
    var fd = fs.openSync(self.r_file_path, 'r', 0755);     //当前文件描述符
    var fid = self.r_file_id;                               //当前文件id
    var offset = self.r_file_size;                                  //当前读取文件偏移量
    var buffer = new Buffer(102400);

    var last_fid = fid;
    var last_fsize = offset;
    var maybe_last_fid = last_fid;
    var maybe_last_fsize = last_fsize;

    var broken = false;
    var expect = 0;     //0:check code;1:size;2:value
    var size = 0;
    var size_buf = new Buffer(2);
    var size_last = 0;
    var value_last = 0;

    while(true) {
        try {
            var bytesRead = fs.readSync(fd, buffer, 0, buffer.length, offset);
        } catch(err) {
            fs.close(fd);
            throw err;
        }

        for(var i=0; i<bytesRead; i++) {
            if(expect==0) {
                if(buffer[i]!=check_code) {
                    broken = true;
                    fs.close(fd);
                    break;
                }
                if(size>0) {
                    self.length++;
                    self.r_last += 3+size;
                    self.r_w_last += 3+size;
                    last_fid = maybe_last_fid;
                    last_fsize = maybe_last_fsize;

                    if(self.length >= self.max_length) {
                        broken = true;
                        fs.close(fd);
                        break;
                    }
                }
                size_last = 2;
                expect = 1;
            } else if(expect==1) {
                if(bytesRead-i>=size_last) {
                    buffer.copy(size_buf, 2-size_last, i, i+size_last);
                    size = size_buf.readUInt16BE(0);
                    if(size<=0) {
                        broken = true;
                        fs.close(fd);
                        break;
                    }
                    i += size_last-1;
                    value_last = size;
                    expect = 2;
                } else {
                    buffer.copy(size_buf, 2-size_last, i, bytesRead);
                    size_last -= bytesRead-i;
                    i += bytesRead-i-1;
                }
            } else {
                if(bytesRead-i>=value_last) {
                    i += value_last-1;
                    maybe_last_fid = fid;
                    maybe_last_fsize = offset+i+1;
                    expect = 0;
                } else {
                    value_last -= bytesRead-i;
                    i += bytesRead-i-1;
                }
            }
        }
        if(broken) {
            break;
        }

        offset += bytesRead;

        // open new file
        if(bytesRead<buffer.length) {
            fs.close(fd);
            
            var next_path = path.join(self.q_path, 'queue_'+(fid+1));
            if(!path.existsSync(next_path)) {
                if(expect==0) {     //normal file end
                    if(size>0) {
                        self.length++;
                        self.r_last += 3+size;
                        self.r_w_last += 3+size;
                        last_fid = maybe_last_fid;
                        last_fsize = maybe_last_fsize;
                    }
                } else {
                    broken = true;
                }
                break;
            }

            self.f_list[fid] = offset;
            fd = fs.openSync(next_path, 'r', 0755);
            fid++;
            offset = 0;
        }
    }
    fs.close(fd);

    
    self.w_file_id = last_fid;
    self.w_file_path = path.join(self.q_path, 'queue_'+self.w_file_id);
    self.w_file_size = last_fsize;

    // clear excess fid
    var excess_fid = self.w_file_id+1;
    while(typeof(self.f_list[excess_fid]) != 'undefined') {
        delete self.f_list[excess_fid];
        excess_fid++;
    }

    //truncate file
    if(broken) {
        var last_fd = fs.openSync(self.w_file_path, 'r+', 0755);
        try {
            fs.truncateSync(last_fd, self.w_file_size);
        } catch(err) {
            fs.close(last_fd);
            throw err;
        }
    }

    self.emit('info', 'last_fid:'+self.w_file_id+' last_fsize:'+self.w_file_size+' queue_length:'+self.length+' file_bytes:'+self.r_last+' broken:'+broken);
    return flags;
};

//start queue service
Queue.prototype.start = function(callback) {
    var self = this;

    if(self.status!=0) {
        var err = new Error('queue\'s status is not stopped');
        self.emit('error', err);
        callback && callback.call(self, err);
        return;
    }

    self.init();
    self.status = 1;

    //mkdir
    var exists = path.existsSync(self.q_path);
    if(!exists) {
        try {
            fs.mkdirSync(self.q_path);
        } catch(err) {
            self.status = 0;
            self.emit('error', err);
            callback && callback.call(self, err);
            return;
        }
    }

    try {
        var flags = self.init_file();
    } catch(err) {
        self.status = 0;
        self.emit('error', err);
        callback && callback.call(self, err);
        return;
    }

    //open file
    //open index file
    try {
        self.i_file = fs.openSync(self.i_file_path, flags, 0775);
    } catch(err) {
        self.status = 0;
        self.emit('error', err);
        callback && callback.call(self, err);
        return;
    }

    try {
        self.w_file = fs.openSync(self.w_file_path, flags, 0755);
    } catch(err) {
        self.i_file = null;

        self.status = 0;
        self.emit('error', err);
        callback && callback.call(self, err);
        return;
    }

    try {
        self.r_file = fs.openSync(self.r_file_path, 'r', 0755);
    } catch(err) {
        self.i_file = null;
        self.w_file = null;

        self.status = 0;
        self.emit('error', err);
        callback && callback.call(self, err);
        return;
    }

    //allocate buffer memory
    self.w_buffer = new Buffer(self.w_buf_size);
    self.r_buffer = new Buffer(self.r_buf_size);

    self.w_timeout_id = setTimeout(function() {
        self.save();
    }, self.w_timeout);

    self.r_timeout_id = setTimeout(function() {
        self.load();
    }, self.r_timeout);

    self.i_timeout_id = setTimeout(function() {
        self.index();
    }, 0);

    //del next file
    var next_path = path.join(self.q_path, 'queue_'+(self.w_file_id+1));
    fs.unlink(next_path);

    self.status = 2;
    self.emit('start');
    callback && callback.call(self);
};

//write queue
Queue.prototype.save = function() {
    var self = this;

    //nothing to save
    if(self.w_unsaved_size<=0) {
        //stopping
        if(self.status==3) {
            fs.close(self.w_file);
            self.w_file = null;
            self.w_buffer = null;
            self.w_timeout_id = null;
            return;
        }

        self.w_timeout_id = setTimeout(function() {
            self.save();
        }, self.w_timeout);
        return;
    }

    var file_last_size = self.file_max_size - self.w_file_size;
    //current file is full
    if(file_last_size<=0) {
        //save current file length
        self.f_list[self.w_file_id] = self.w_file_size;

        //close current file
        fs.close(self.w_file);

        //open new file
        self.w_file_id++;
        self.w_file_path = path.join(self.q_path, 'queue_'+self.w_file_id);
        self.w_file_size = 0;
        
        fs.open(self.w_file_path, 'w', 0755, function(err, fd) {
            if(err) {
                self.w_file = null;
                self.w_buffer = null;
                self.w_timeout_id = null;

                self.emit('error', err);
                self.stop();
                return;
            }
            self.w_file = fd;
            self.save();
        });

        //del next file
        var next_path = path.join(self.q_path, 'queue_'+(self.w_file_id+1));
        fs.unlink(next_path);

        return;
    }

    //save
    var w_last_size;
    //loop
    if(self.w_buf_size-self.wf_rear < self.w_unsaved_size) {
        w_last_size = self.w_buf_size - self.wf_rear;
    } else {
        w_last_size = self.w_unsaved_size;
    }
    w_last_size = w_last_size<file_last_size ? w_last_size : file_last_size;
    fs.write(self.w_file, self.w_buffer, self.wf_rear, w_last_size, self.w_file_size, function(err, written, buffer) {
        if(err) {
            fs.close(self.w_file);
            self.w_file = null;
            self.w_buffer = null;
            self.w_timeout_id = null;

            self.emit('error', err);
            self.stop();
            return;
        }

        self.w_unsaved_size -= written;
        self.w_file_size += written;
        self.wf_rear += written;
        if(self.wf_rear >= self.w_buf_size) {
            self.wf_rear -= self.w_buf_size;
        }
        self.r_last += written;
        self.w_timeout_id = setTimeout(function() {
            self.save();
        }, 0);
    });
};

//enQueue
Queue.prototype.enQueue = function(value) {
    var self = this;

    if(!value || value.length<=0) {
        throw new Error('value is empty');
    }

    if(self.status!=2) {
        throw new Error('queue is not started');
    }

    var size = value.length;
    if(size>0xFFFF) {
        throw new Error('value length is too big(don\'t exceed 65535 bytes)');
    }
    if(size+3>self.w_buf_size-self.w_unsaved_size) {
        throw new Error('write buffer is full');
    }
    if(self.length >= self.max_length) {
        throw new Error('queue is full');
    }

    var size_buf = new Buffer(3);
    size_buf.writeUInt8(check_code, 0);
    size_buf.writeUInt16BE(size, 1);
    var rear;
    if(self.w_buf_size>=self.w_rear+3) {
        rear = self.w_rear+3;
        size_buf.copy(self.w_buffer, self.w_rear, 0, 3);
    }else {
        rear = 3-(self.w_buf_size-self.w_rear);
        size_buf.copy(self.w_buffer, self.w_rear, 0, 3-rear);
        size_buf.copy(self.w_buffer, 0, 3-rear, 3);
    }
    if(self.w_buf_size>=rear+size) {
        self.w_rear = rear+size;
        value.copy(self.w_buffer, rear, 0, size);
    }else {
        self.w_rear = size-(self.w_buf_size-rear);
        value.copy(self.w_buffer, rear, 0, size-self.w_rear);
        value.copy(self.w_buffer, 0, size-self.w_rear, size);
    }

    if(self.w_rear>=self.w_buf_size) {
        self.w_rear -= self.w_buf_size;
    }
    self.length++;
    self.w_unsaved_size += 3+size;
    self.w_used_size += 3+size;
    if(self.w_used_size > self.w_buf_size) {
        self.w_front += self.w_used_size-self.w_buf_size;
        if(self.w_front >= self.w_buf_size) {
            self.w_front -= self.w_buf_size;
        }
        self.r_w_last += self.w_used_size-self.w_buf_size;
        self.w_used_size = self.w_buf_size;
    }
};

//read queue
Queue.prototype.load = function() {
    var self = this;

    //stopping
    if(self.status==3) {
        fs.close(self.r_file);
        self.r_file = null;
        self.r_buffer = null;
        self.r_timeout_id = null;
        return;
    }

    var r_last_size = self.r_buf_size - self.r_used_size;
    r_last_size = r_last_size<(self.r_buf_size-self.r_rear) ? r_last_size : (self.r_buf_size-self.r_rear);
    r_last_size = r_last_size<self.r_last ? r_last_size : self.r_last;

    //r_file full or empty file
    if(r_last_size<=0) {
        self.r_timeout_id = setTimeout(function() {
            self.load();
        }, self.r_timeout);
        return;
    }

    //load
    fs.read(self.r_file, self.r_buffer, self.r_rear, r_last_size, self.r_file_size, function(err, bytesRead, buffer) {
        if(err) {
            fs.close(self.r_file);
            self.r_file = null;
            self.r_buffer = null;
            self.r_timeout_id = null;

            self.emit('error', err);
            self.stop();
            return;
        }

        self.r_rear += bytesRead;
        if(self.r_rear >= self.r_buf_size) {
            self.r_rear -= self.r_buf_size;
        }
        self.r_used_size += bytesRead;
        self.r_last -= bytesRead;
        self.r_w_last -= bytesRead;
        self.r_file_size += bytesRead;

        //file loop
        if(bytesRead<r_last_size) {
            //close current file
            fs.close(self.r_file);

            //open new file
            self.r_file_id++;
            self.r_file_path = path.join(self.q_path, 'queue_'+self.r_file_id);
            self.r_file_size = 0;

            fs.open(self.r_file_path, 'r', 0755, function(err, fd) {
                if(err) {
                    self.r_file = null;
                    self.r_buffer = null;
                    self.r_timeout_id = null;
 
                    self.emit('error', err);
                    self.stop();
                    return;
                }
                self.r_file = fd;
                self.load();
            });
            return;
        }

        self.r_timeout_id = setTimeout(function() {
            self.load();
        }, 0);
    });
};

//deQueue
Queue.prototype.deQueue = function() {
    var self = this;

    if(self.length<=0) {
        return null;
    }
    if(self.status!=2) {
        throw new Error('queue is not started');
    }

    if(self.r_used_size<3) {
        throw new Error('read buffer is empty');
    }
    var size_buf = new Buffer(3);
    var front;
    if(self.r_buf_size>=self.r_front+3) {
        front = self.r_front+3;
        self.r_buffer.copy(size_buf, 0, self.r_front, front);
    }else {
        front = 3-(self.r_buf_size-self.r_front);
        self.r_buffer.copy(size_buf, 0, self.r_front, self.r_buf_size);
        self.r_buffer.copy(size_buf, 3-front, 0, front);
    }
    //check code
    if(size_buf.readUInt8(0)!=check_code) {
        throw new Error('check code error');
    }
    var size = size_buf.readUInt16BE(1);

    if(self.r_used_size<3+size) {
        throw new Error('read buffer is empty');
    }
    var value = new Buffer(size);
    if(self.r_buf_size>=front+size) {
        self.r_front = front+size;
        self.r_buffer.copy(value, 0, front, self.r_front);
    }else {
        self.r_front = size-(self.r_buf_size-front);
        self.r_buffer.copy(value, 0, front, self.r_buf_size);
        self.r_buffer.copy(value, size-self.r_front, 0, self.r_front);
    }
    if(self.r_front>=self.r_buf_size) {
        self.r_front -= self.r_buf_size;
    }
    self.length--;
    self.r_used_size -= 3+size;

    self.r_front_file_offset += 3+size;
    while(self.f_list[self.r_front_file_id]<self.r_front_file_offset) {
        self.r_front_file_offset -= self.f_list[self.r_front_file_id];
        self.r_front_file_id++;
    }
    return value;
};

//index
Queue.prototype.index = function() {
    var self = this;

    //nothing to save
    if(self.r_front_file_id_old==self.r_front_file_id && self.r_front_file_offset_old==self.r_front_file_offset) {
        //stopping
        if(self.status==3) {
            fs.close(self.i_file);
            self.i_file = null;
            self.i_timeout_id = null;
            return;
        }

        self.i_timeout_id = setTimeout(function() {
            self.index();
        }, self.i_timeout);
        return;
    }

    self.r_front_file_id_old = self.r_front_file_id;
    self.r_front_file_offset_old = self.r_front_file_offset;

    var index_buf = new Buffer(self.r_front_file_id+','+self.r_front_file_offset+',');
    fs.write(self.i_file, index_buf, 0, index_buf.length, 0, function(err, written, buffer) {
        if(err) {
            fs.close(self.i_file);
            self.i_file = null;
            self.i_timeout_id = null;

            self.emit('error', err);
            self.stop();
            return;
        }
        fs.truncate(self.i_file, index_buf.length, function(err) {
            if(err) {
                fs.close(self.i_file);
                self.i_file = null;
                self.i_timeout_id = null;

                self.emit('error', err);
                self.stop();
                return;
            }
            //del old file
            for(var i=self.r_front_file_id_old-1; i>=0 && typeof(self.f_list[i])!='undefined'; i--) {
                var old_path = path.join(self.q_path, 'queue_'+i);
                fs.unlink(old_path);
                delete self.f_list[i];
            }
            self.i_timeout_id = setTimeout(function() {
                self.index();
            }, self.i_timeout);
        });
    });

};

//stop queue service
Queue.prototype.stop = function(callback) {
    var self = this;

    //closed or closing
    if(self.status==0 || self.status==3) {
        callback && callback.call(self);
        return;
    }
    //starting
    if(self.status==1) {
        self.once('start', function() {
            self.stop(callback);
        });
        return;
    }

    self.status=3;

    if(!self.w_timeout_id) {
        if(self.w_file) {
            fs.close(self.w_file);
            self.w_file = null;
        }
        self.w_buffer = null;
    }
    if(!self.r_timeout_id) {
        if(self.r_file) {
            fs.close(self.r_file);
            self.r_file = null;
        }
        self.r_buffer = null;
    }
    if(!self.i_timeout_id) {
        if(self.i_file) {
            fs.close(self.i_file);
            self.i_file = null;
        }
    }

    var wait_stop = function () {
        if(self.status!=3) {
            callback && callback.call(self);
            return;
        }
        if(!self.w_file && !self.w_buffer && !self.w_timeout_id && !self.r_file && !self.r_buffer && !self.r_timeout_id && !self.i_file && !self.i_timeout_id) {
            self.status = 0;
            self.emit('stop');
            callback && callback.call(self);
            return;
        }
        setTimeout(wait_stop, 10);
    };
    setTimeout(wait_stop, 10);
};

//exports
module.exports = Queue;
