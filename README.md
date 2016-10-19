# 结合workman-jsonrpc和jsonrpc打造更赛艇的JsonRPC框架
workerman
=========

workerman 是一个高性能的PHP socket服务框架，开发者可以在这个框架下开发各种网络应用,例如RPC服务、聊天室、游戏等。
workerman 具有以下特性
 * 多进程
 * 支持TCP/UDP
 * 支持各种应用层协议
 * 使用libevent事件轮询库，支持高并发
 * 支持文件更新检测及自动加载
 * 支持服务平滑重启
 * 支持telnet远程控制及监控
 * 支持异常监控及告警
 * 支持长连接
 * 支持以指定用户运行worker进程

 [更多请访问www.workerman.net](http://www.workerman.net/workerman-jsonrpc)


RPC应用使用方法
=========

###客户端同步调用：

```php
<?php
include_once 'yourClientDir/RpcClient.php';

$address_array = array(
          'tcp://127.0.0.1:2015',
          'tcp://127.0.0.1:2015'
          );
// 配置服务端列表
RpcClient::config($address_array);

$uid = 567;

// User对应applications/JsonRpc/Services/User.php 中的User类
$user_client = RpcClient::instance('User');

// getInfoByUid对应User类中的getInfoByUid方法
$ret_sync = $user_client->getInfoByUid($uid);

```

###客户端异步调用：
RpcClient支持异步远程调用

```php
<?php
include_once 'yourClientDir/RpcClient.php';
// 服务端列表
$address_array = array(
  'tcp://127.0.0.1:2015',
  'tcp://127.0.0.1:2015'
  );
// 配置服务端列表
RpcClient::config($address_array);

$uid = 567;
$user_client = RpcClient::instance('User');

// 异步调用User::getInfoByUid方法
$user_client->asend_getInfoByUid($uid);
// 异步调用User::getEmail方法
$user_client->asend_getEmail($uid);

这里是其它的业务代码
....................
....................

// 需要数据的时候异步接收数据
$ret_async1 = $user_client->arecv_getEmail($uid);
$ret_async2 = $user_client->arecv_getInfoByUid($uid);

这里是其他业务逻辑

```

###服务端：  
服务端每个类提供一组服务，类文件默认放在Applications/JsonRpc/Entities目录下。  
客户端实际上是远程调用这些类的静态方法。
例如：
```php
<?php
RpcClient::instance('User')->getInfoByUid($uid);
```
调用的是Applications/JsonRpc/Entities/User.php 中 User类的getInfoByUid方法。    
User.php文件类似这样
```php
<?php
class User
{
       public static function getInfoByUid($uid)
        {
            // ....
        }
   
        public static function getEmail($uid)
        {
            // ...
        }
}
```

如果你想要增加一组服务，可以在这个目录下增加类文件即可。

rpc监控页面
======
rpc监控页面地址 http://ip:55757

