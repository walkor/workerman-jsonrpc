<?php
namespace Redis;

/*
 * 如果你在一个项目里面用到了很多个集群，那么用这个
 */

/**
 * Description of RedisMultiStorage
 *
 * @author guoxinhua
 */
class RedisMultiCache {

    public static $instance;
    public static $config;

    /**
     * @param $name 配置名称
     * @return \Redis\RedisCache
     */
    public static function getInstance($name) {
        if(!static::$config)
        {
            static::$config = (array) new \Config\Redis();
            RedisCache::config(static::$config);
        }
        if (!isset(static::$instance[$name])) {
            static::$instance[$name] = RedisCache::getInstance($name);
        }
        return static::$instance[$name];
    }

    public static function config(array $config) {
        static::$config = $config;
        RedisCache::config(static::$config);
    }

    public static function close(){
        $closeExMsg = null;
        foreach ((array)static::$instance as $inst) {
            try {
                $inst->close();
            }
            catch(\Exception $ex)
            {
                $closeExMsg[] = $ex->getMessage();
            }
        }
        if($closeExMsg)
        {
            throw new \RedisException(implode("\n", $closeExMsg), 2, $ex);
        }
        static::$instance = array();
    }

}
