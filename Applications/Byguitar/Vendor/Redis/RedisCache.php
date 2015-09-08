<?php

namespace Redis;

use MNLogger\TraceLogger;
use Exception;

/**
 * 当cache用的redis
 *
 * @author xinhuag@jumei.com
 */
class RedisCache extends RedisBase {
    /*
     * hash类的引用
     */

    private $hash;
    private $MasterOrSlave;
    protected static $configs;
    /*
     * 配置
     */
    public $config;
    /*
     * redis实例
     */
    public $redis;
    /*
     * 单例
     */
    private static $instance;

    protected function __construct() {
        if (defined('JM_PHP_CONN_POOL_ON')) {
            $this->usePool = (bool) JM_PHP_CONN_POOL_ON;
        };
        parent::__construct();
    }

    public static function getInstance($name = 'default') {
        if (!isset(self::$instance[$name])) {
            self::$instance[$name] = new self;
        }

        if (!static::$configs) {
            static::$configs = new \Config\Redis();
        }

        if (!self::$instance[$name]->config) {
//            self::$instance[$name]->config = static::$configs;
            self::$instance[$name]->config = static::$configs[$name];
        }
        self::$instance[$name]->Init();
        return self::$instance[$name];
    }

    public static function config($config = null) {
        if (empty($config)) {
            return static::$configs;
        }
        static::$configs = $config;
    }

    public function close() {
        $failedTargets = array();
        foreach ((array) $this->redis as $target => $value) {
            try {
                unset($this->redis[$target]);
                $this->usePool ? $value->release() : $value->close();
            } catch (Exception $exc) {
                $failedTargets[] = $target;
            }
        }
        if (!empty($failedTargets)) {
            throw new \RedisException('Redis close error. last closing: ' . $exc->getMessage() . '. Failed targets: ' . implode(',', $failedTargets));
        }
    }

    public function Init() {
        $this->hash = new ConsistentHash();
        $this->MasterOrSlave = $ShmList = ShmConfig::getCacheAvailableAddress($this->config); //从内存中获得可用列表
        $list = array();
        if (empty($ShmList)) {//内存中没有，可能ping脚本没启,直接用配置
            foreach ($this->config['nodes'] as $node) {
                $list[] = $node['master'];
            }
        } else {
            foreach ($ShmList as $node) {//false已过滤,主/从在逻辑上都hash主的值
                $list[] = $node['master']['target'];
            }
        }
        $this->hash->addTargets($list); //传入逻辑结点列表
    }

    /*
     * 根据key和实际结点建立链接
     */

    public function ConnectTarget($key) {
        $target = $this->hash->lookup($key);
        foreach ($this->MasterOrSlave as $node) {
            if (strcmp("slave", $node['use']) === 0 && strcmp($target, $node['master']['target']) === 0) {//因为缓存也做了主从，所以主挂了逻辑上可用，但是实际得用从
                $target = $node['slave']['target'];
            }
        }
        $this->target = $target;
        $this->real_connect($target, $key);
        return $this->redis[$target];
    }

}
