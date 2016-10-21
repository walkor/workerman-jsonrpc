<?php
/**
 * run with command 
 * php start.php start
 */
define('APP',__DIR__'/Apps');
$registy = array(
    'jsonrpc'=>APP.'/JsonRPC/',
    'statistics'=>APP.'/Statistics/'
);
if($argc == 0)
    exit("请至少输入一个启动的服务");

ini_set('display_errors', 'on');
use Workerman\Worker;

// 检查扩展
if(!extension_loaded('pcntl'))
{
    exit("缺少PCNTL扩展");
}

if(!extension_loaded('posix'))
{
    exit("缺少POSIX扩展");
}

// 标记是全局启动
define('GLOBAL_START', 1);

require_once __DIR__ . '/Workerman/Autoloader.php';

// 加载所有Applications/*/start.php，以便启动所有服务
foreach($argv as $index => $start_file)
{
    if($index == 0)
        continue;
    if(in_array($start_file,$registy)){
        require_once $registy[$start_file];
    }else{
        echo "服务 $start_file 不存在".PHP_EOL;
    }
}
// 运行所有服务
Worker::runAll();