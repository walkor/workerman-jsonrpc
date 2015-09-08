<?php
namespace Redis;
use MNLogger\TraceLogger;
use Exception;

/**
 *
 * @author xinhuag@jumei.com
 */
abstract class RedisBase {

    const STRING = 1;
    const SET = 2;
    const LISTS = 3;
    const ZSET = 4;
    const HASH = 5;

    /*
     * 是否是事务    
     */

    public $isTransaction = false;

    /*
     * 是事务key和方法緩存    
     */
    public $TransactionCache = array();

    /*
     * 所有的读操作
     */
    protected $ReadFun = array(
    );
    /*
     * 所有的写操作
     */
    protected $WriteFun = array(
    );
    protected $usePool = true;

    protected function __construct() {
        if (!class_exists('redis_connect_pool')) {
            $this->usePool = false;
        }
    }

    /*
     * 暂时不支持的函数
     */
    protected $DisableFun = array(
        "KEYS", "BLPOP", "MSETNX", "BRPOP", "RPOPLPUSH", "BRPOPLPUSH", "SMOVE", "SINTER", "SINTERSTORE", "SUNION", "SUNIONSTORE", "SDIFF", "SDIFFSTORE", "ZINTER", "ZUNION",
        "FLUSHDB", "FLUSHALL", "RANDOMKEY", "SELECT", "MOVE", "RENAMENX", "DBSIZE", "BGREWRITEAOF", "SLAVEOF", "SAVE", "BGSAVE", "LASTSAVE"
    );

    /*
     * 本次调用的具体物理机,用于调试
     */
    protected $target = '';

    public function __call($name, $arguments) {
        if ($this->isTransaction) {//事務緩存
            $this->TransactionCache[] = array('name' => $name, 'arg' => $arguments);
            return true;
        }
        if (in_array(strtoupper($name), $this->DisableFun)) {
            throw new Exception("call the disable function!");
        }

        $obj = $this->ConnectTarget($arguments[0]);
        if (empty($obj)) {//节点失效了，但是ping还没踢掉呢
            return false;
        }

        $exCaller = $this::getExternalCaller();
        // 需先初始化MNLogger
        $logger = TraceLogger::instance('trace');
        $logger->REDIS_CS($this->target, $exCaller['class'] . '::' . $exCaller['method'], serialize($arguments));
        try {
            $ret = call_user_func_array(array($obj, $name), $arguments);
        } catch (Exception $ex) {
            if ($this->usePool) {
                $obj->release();
            }
            throw new Exception('Redis operation error. node: '.$arguments[0].' details: '.$ex->getMessage(), 32001, $ex);
            $logger->REDIS_CR("EXCEPTION",strlen(serialize($ret)));
        }
        if ($this->usePool) {
            $obj->release();
        }
        $logger->REDIS_CR("SUCCESS", strlen(serialize($ret)));

        return $ret;
    }

    public function MULTI() {
        $this->isTransaction = true;
        return true;
    }

    public function EXEC() {
        $this->isTransaction = false;
        $key = null;
        foreach ((array) $this->TransactionCache as $cache) {//check key
            $arguments = $cache['arg'];
            if (strcmp($key, $arguments[0]) !== 0 && isset($key)) {
                throw new Exception("Transaction error!Need same key but multi key passed");
            }
            $key = $arguments[0];
        }
        $obj = $this->ConnectTarget($key);
        $obj->MULTI();
        foreach ((array) $this->TransactionCache as $cache) {
            call_user_func_array(array($obj, $cache['name']), $cache['arg']);
        }
        unset($this->TransactionCache);
        $ret = $obj->EXEC();
        if ($this->usePool) {
            $obj->release();
        }
        return $ret;
    }

    public function real_connect($target, $key) {
        if (!isset($this->redis[$target])) {//每个物理机对应一个new redis
            try {
                $logger = TraceLogger::instance('trace');
                if ($this->usePool) {
                    $this->redis[$target] = new \redis_connect_pool();
                } else {
                    $this->redis[$target] = new \Redis();
                }
                $ip_port = explode(":", $target);
                $logger->REDIS_CS($ip_port[0] . ':' . $ip_port[1], get_class($this->redis[$target]) . '::connect', '');
                $this->redis[$target]->connect($ip_port[0], $ip_port[1], 10);
                $logger->REDIS_CR('success', 0);
                if (isset($this->config['db'])) {//如果设置了db
                    $this->redis[$target]->select($this->config['db']);
                }
            } catch (Exception $e) {
                $logger->REDIS_CR('exception', 0);
                if (strstr($e->getMessage(), "connect to pool_server fail")) {//端口连不上
                    $this->redis[$target] = new \Redis();
                    $ip_port = explode(":", $target);
                    $logger->REDIS_CS($ip_port[0] . ':' . $ip_port[1], get_class($this->redis[$target]) . '::connect', '');
                    $success = $this->redis[$target]->connect($ip_port[0], $ip_port[1], 10);
                    if ($success === true) {
                        $logger->REDIS_CR('success', 0);
                    } else {
                        $logger->REDIS_CR('exception', 0);
                    }
                    if (isset($this->config['db'])) {//如果设置了db
                        $this->redis[$target]->select($this->config['db']);
                    }
                    $this->usePool = false;
                } else {
                    unset($this->redis[$target]); //对象销毁会自动调用release
                    throw new Exception("Connect redis error!\nKey: " . $key . "\nTarget:" . $target . "\nDB:" . $this->config['db']);
                }
            }
        }
    }

    /*
     * 分布式缓存需要特殊处理
     * 尽量少用,可以用集合代替呀
     */

    public function Mget(array $keys) {
        $ret = array();
        foreach ($keys as $key) {
            $obj = $this->ConnectTarget($key); //返回redis对象
            if (!$obj)//链接失败
                continue;
            $ret[] = $obj->get($key);
        }
        return $ret;
    }

    public function getMultiple(array $keys) {
        return $this->Mget($keys);
    }

    public function Mset(array $KeyValue) {
        $ObjValue = array();
        $ObjArr = array(); //对象数组
        $socketNum = 0;
        foreach ($KeyValue as $key => $value) {
            $obj = $this->ConnectTarget($key); //返回redis对象
            if (!$obj)//链接失败
                continue;
            $ObjArr[$socketNum] = $obj;
            $ObjValue[$socketNum][$key] = $value;
            $socketNum++;
        }
        foreach ($ObjValue as $socketNum => $kv) {
            $obj = $ObjArr[$socketNum];
            if (!$obj->mset($kv)) {
                return false;
            };
        }
        return true;
    }

    public function delete($key) {
        if (is_array($key)) {
            foreach ($key as $k) {
                $redis = $this->ConnectTarget($k);
                if (!$redis)//链接失败
                    continue;
                $redis->delete($k);
            }
        } else {
            $redis = $this->ConnectTarget($key); //返回redis对象
            $redis->delete($key);
        }
        return true;
    }

    public function GetTarget() {
        return $this->target;
    }

    /*
     * rename前端app用
     * $key1 原key
     * $key2 生成的新key
     * return 原来key的值
     */

    public function rename($key1, $key2) {
        $redis = $this->ConnectTarget($key1);
        $target1 = $this->target;
        $this->ConnectTarget($key2);
        $target2 = $this->target;
//        if ((int) $redis->socket === (int) $redisTarget->socket) {//key1,key2刚好在一台机器
        if (strcmp($target1, $target2) === 0) {//key1,key2刚好在一台机器
            return $redis->rename($key1, $key2);
        }
        $type = $redis->type($key1);
        switch ($type) {
            case self::STRING:
                return $this->renameString($key1, $key2);
            case self::SET:
                return $this->renameSet($key1, $key2);
            case self::LISTS:
                return $this->renameList($key1, $key2);
            case self::ZSET:
                return $this->renameZSet($key1, $key2);
            case self::HASH:
                return $this->renameHash($key1, $key2);
            default:
                return false;
        }
    }

    private function renameString($key1, $key2) {
        $redis = $this->ConnectTarget($key1);
        $data = $redis->get($key1);
        if ($data !== false) {
            $redisTarget = $this->ConnectTarget($key2);
            $redisTarget->delete($key2);
            if ($redisTarget->set($key2, $data) === FALSE) {
                return false;
            }
        } else {
            return false;
        }
        $redis->delete($key1);
        return true;
    }

    private function renameSet($key1, $key2) {
        $redis = $this->ConnectTarget($key1);
        $data = $redis->sMembers($key1);
        if ($data) {
            $redisTarget = $this->ConnectTarget($key2);
            $redisTarget->delete($key2);
            foreach ($data as $value) {
                if ($redisTarget->sadd($key2, $value) === FALSE) {
                    return FALSE;
                }
            }
        } else {
            return FALSE;
        }
        $redis->delete($key1);
        return true;
    }

    private function renameList($key1, $key2) {
        $redis = $this->ConnectTarget($key1);
        $data = $redis->lRange($key1, 0, -1);
        if ($data) {
            $redisTarget = $this->ConnectTarget($key2);
            $redisTarget->delete($key2);
            foreach ($data as $value) {
                if ($redisTarget->rPush($key2, $value) === FALSE) {
                    return false;
                }
            }
        } else {
            return FALSE;
        }
        $redis->delete($key1);
        return true;
    }

    private function renameZSet($key1, $key2) {
        $redis = $this->ConnectTarget($key1);
        $data = $redis->zRange($key1, 0, -1, true);
        if ($data) {
            $redisTarget = $this->ConnectTarget($key2);
            $redisTarget->delete($key2);
            foreach ($data as $value => $score) {
                if ($redisTarget->zadd($key2, $score, $value) === FALSE) {
                    return FALSE;
                }
            }
        } else {
            return FALSE;
        }
        $redis->delete($key1);
        return true;
    }

    private function renameHash($key1, $key2) {
        $redis = $this->ConnectTarget($key1);
        $data = $redis->hGetAll($key1);
        if ($data) {
            $redisTarget = $this->ConnectTarget($key2);
            $redisTarget->delete($key2);
            if ($redisTarget->hMset($key2, $data) === FALSE) {
                return FALSE;
            }
        } else {
            return FALSE;
        }
        $redis->delete($key1);
        return true;
    }

    abstract public function ConnectTarget($key); //redis对象池

    abstract public function Init();

    /**
     * 获取外部调用者.
     *
     * @return array array('file'=>'...', 'line'=>'...', 'method'=>'...', 'class'=>'..')
     */
    public static function getExternalCaller() {
        $trace = debug_backtrace(false);
        $caller = array('class' => '', 'method' => '', 'file' => '', 'line' => '');
        $k = 0;
        foreach ($trace as $k => $line) {
            if (isset($line['class']) && strpos($line['class'], __NAMESPACE__) === 0) {
                continue;
            } else if (isset($line['class'])) {
                $caller['class'] = $line['class'];
                $caller['method'] = $line['function'];
            } else if (isset($line['function'])) {
                $caller['method'] = $line['function'];
            } else {
                $caller['class'] = 'main';
            }
            break;
        }
        if (empty($caller['method'])) {
            $caller['method'] = 'main';
        }
        while (!isset($line['file']) && $k > 0) {// 可能在eval或者call_user_func里调用的。
            $line = $trace[--$k];
        }
        $caller['file'] = $line['file'];
        $caller['line'] = $line['line'];
        return $caller;
    }
}
