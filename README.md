# NTMQ是一个基于Node的轻量化消息队列

## 支持Node版本

Node version >= v0.5.5

## 功能特性

* 只需Node运行环境，无任何其他运行环境、第三方库的依赖
* 使用磁盘扩充队列存储，并持久化
* 基于内存的进出队Buffer，提供最快的读写响应时间，对瞬间大访问量有效削峰
* 当瞬间流量太大超过Buffer和磁盘持久化可以承受的限制时，采用放弃一部分请求的策略，确保大部分请求的响应速度
* 支持多队列
* 支持二进制消息
* 可扩充的访问协议支持。目前暂时只支持HTTP RESTful
* 鲁棒性与自修复能力。在服务异常终止（kill -9或死机）后重新启动时自动检测文件的完整性，确保数据一致及最小的消息丢失

## 功能限制

* 不支持消息订阅
* 单进程，只能利用一个CPU Core。当然也可以启多个进程实现负载均衡
* 服务异常终止有可能造成少量消息的丢失或重复获取（视持久化时间间隔长短而定）

## 使用方法

参见[test_server.js](test_server.js)。可以直接修改此文件为你的服务程序，作为独立的服务运行。也可以在你的程序中`require('ntmq')`嵌入消息队列的功能。

## 协议说明

### 入队Request:

    POST/PUT http://host:port/queue_name

HTTP Body为未编码二进制消息体

### 入队Response:

* HTTP Code: 200

  入队成功，返回`'ok'`

* HTTP Code: 413

  HTTP Body超过长度限制。目前的长度限制为0xFFFF字节

* HTTP Code: 500

  其他服务器端错误，返回错误说明。最有可能的错误是入队Buffer已满，稍等重试即可

### 出队Request:

    GET http://host:port/queue_name

### 出队Response:

* HTTP Code: 200

  出队成功，返回一个消息。如果返回内容为空，表示当前队列空，没有未读消息

* HTTP Code: 500

  其他服务器端错误，返回错误说明。最有可能的错误是队列中有未读消息，但读文件速度不够快造成出队Buffer空。稍等重试即可 

## 功能测试

默认带一个简单（简陋）的功能测试页，测试服务是否正常

    http://host:port/test

## 性能测试

在我的MBP（2.4GHz 2 Core/8G Mem/5400RPM HD）上用siege进行benchmark测试，开keep-alive，读写均为近7000tps，response time小于10ms无显示。欢迎大家在各种环境下测试，并反馈测试结果

## 下一步计划

* 单元测试
* 支持一种二进制协议，进一步提高性能
* 提供Client端API封装 
