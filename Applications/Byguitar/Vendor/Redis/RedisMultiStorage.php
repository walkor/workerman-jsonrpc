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
class RedisMultiStorage {

    private static $instance;
    private static $config;

    public static function getInstance($name) {
        if(!self::$config)
        {
            self::$config = (array) new \Config\Redis();
            RedisStorage::config(static::$config);
        }

        if (!isset(static::$instance[$name])) {
            static::$instance[$name] = RedisStorage::getInstance($name);
        }
        return self::$instance[$name];
    }

    public static function config(array $config) {
        self::$config = $config;
        RedisStorage::config(static::$config);
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
