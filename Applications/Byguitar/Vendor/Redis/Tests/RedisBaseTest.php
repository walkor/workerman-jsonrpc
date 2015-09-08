<?php
require 'common.php';
class RedisBaseTest extends \PHPUnit_Framework_TestCase
{
    /**
     *
     */
    public function testGet()
    {
        $cache = \Redis\RedisMultiCache::getInstance('default');
        $storage = \Redis\RedisMultiStorage::getInstance('default');
        $key = __CLASS__.'-'.__METHOD__;
        $data = 'storage:'.date('Y-m-d H:i:s');
        $storage->set($key, $data);
        $logger = new MNLoggerCallerTest();
        $returnData = $logger->testCall1(array($storage, 'get'), array($key));
        echo "Test data read from rediststorage: ".var_export($returnData,true)."\n";
        $this->assertEquals($data, $returnData);
        $data = 'cache:'.date('Y-m-d H:i:s');
        $cache->set($key, $data);
        $returnData = $logger->testCall1(array($cache, 'get'), array($key));
        echo "Test data read from redistcache: ".var_export($returnData,true)."\n";
        $this->assertEquals($data, $returnData);
    }

    public function testMget()
    {
        $key = __CLASS__.'-'.__METHOD__;
        \Redis\RedisMultiCache::getInstance('default')->mSet(array("{$key}_1"=>32, "{$key}_2"=>321));
        \Redis\RedisMultiCache::getInstance('default')->mGet(array("{$key}_1", "{$key}_2"));
    }

}


class MNLoggerCallerTest{
    public function testCall1($callback, $args)
    {
        return call_user_func_array($callback, $args);
    }
}
