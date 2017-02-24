<?php
/**
 * Redis 配置
 */
namespace Config;
if(!defined('JM_PHP_CONN_POOL_ON'))define('JM_PHP_CONN_POOL_ON', "#{Res.php-connectionpool.global.enable}");

class Redis{
    /**
     * Configs of Redis.
     * @var array
     */
    public $default = array('nodes' => array(
        array('master' => "127.0.0.1:6379", 'slave' => "127.0.0.1:6379"),
//            array('master' => "192.168.8.230:27004", 'slave' => "192.168.8.231:27004"),

    ),
        'db' => 0
    );
    public $fav = array('nodes' => array(
        array('master' => "127.0.0.1:6379", 'slave' => "127.0.0.1:6379"),
    ),
        'db' => 2
    );
}
