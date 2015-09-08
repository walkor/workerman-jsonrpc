<?php
namespace Redis;

/**
 *
 * 共享内存配置
 *
 * REDIS_SHM_KEY
 * 业务自己定义一个不和别人冲突的key
 * 使用方法在最下面有示例
 *
 */
class ShmConfig {

    /**
     * @var int
     */
    const REDIS_SHM_KEY = 0x19890154;

    /**
     * 缓存的key
     * @var int
     */
    const SHM_CACHE_KEY = 0;

    /**
     * 存储的key
     * @var int
     */
    const SHM_STORAGE_KEY = 1;

    /**
     * 原始的cache配置，用来查看变化
     * @var int
     */
    const SHM_CACHE_ORIGINAL_KEY = 10;

    /**
     * 原始的存储配置，用来查看变化
     * @var int
     */
    const SHM_STORAGE_ORIGINAL_KEY = 11;

    /**
     * 配置
     * @var array
     */
    public static $CacheConfig = null;
    public static $StorageConfig = null;

    /**
     * 共享内存fd
     * @var resource
     */
    private static $AddressShmFd = null;

    /*
     * 301前不用共享内存
     */
    public static $UseMem = false;

    /**
     * @param array $config
     * @return array
     */
    public static function configCache(array $config = array()) {
        if (!empty($config)) {
            self::getShmFd();
            self::$CacheConfig = $config;
            // 用来判断配置是否有更新
            $configOld = shm_get_var(self::$AddressShmFd, self::SHM_CACHE_ORIGINAL_KEY);
            if (serialize($config) === serialize($configOld)) {
                return;
            }
            $arr = self::makeStr2Arr($config);
            shm_put_var(self::$AddressShmFd, self::SHM_CACHE_KEY, $arr);
            shm_put_var(self::$AddressShmFd, self::SHM_CACHE_ORIGINAL_KEY, $config);
        }
        return self::$CacheConfig;
    }

    /**
     * @param array $config
     * @return array
     */
    public static function configStorage(array $config = array()) {
        if (!empty($config)) {
            self::getShmFd();
            self::$StorageConfig = $config;
            // 用来判断配置是否有更新
            $configOld = shm_get_var(self::$AddressShmFd, self::SHM_STORAGE_ORIGINAL_KEY);
            if (serialize($config) === serialize($configOld)) {
                return;
            }
            $arr = self::makeStr2Arr($config);
            shm_put_var(self::$AddressShmFd, self::SHM_STORAGE_KEY, $arr);
            shm_put_var(self::$AddressShmFd, self::SHM_STORAGE_ORIGINAL_KEY, $config);
        }
        return self::$StorageConfig;
    }

    /*
     * 第一次set进来生成相应的数据结构
     */

    public static function makeStr2Arr($AllAddress) {
        foreach ($AllAddress['nodes'] as &$node) {
            $target_m = $node['master'];
            $target_s = $node['slave'];
            $node['master'] = array(
                'target' => $target_m,
                'total' => 0,
                'wrong' => 0
            );
            $node['slave'] = array(
                'target' => $target_s,
                'total' => 0,
                'wrong' => 0
            );
            $node['use'] = 'master'; //第一次进来使用master
        }
        return $AllAddress;
    }

    /**
     * 获取故障节点共享内存的Fd
     * @return resource
     */
    public static function getShmFd() {
        if (!self::$AddressShmFd) {
            self::$AddressShmFd = shm_attach(self::REDIS_SHM_KEY, 10000, 0777);
        }
        return self::$AddressShmFd;
    }

    public static function getCacheAvailableAddress($config) {
        if (self::$UseMem) {
            ShmConfig::configCache($config);
            $nodes = shm_get_var(self::getShmFd(), self::SHM_CACHE_KEY);
            $ret = array();
            foreach ((array) $nodes['nodes'] as $node) {
                if (!isset($node['use'])) {//ping没启动
                    break;
                }
                if ($node['use']) {//use=false证明m-s都失效了
                    $ret[] = $node;
                }
            }
            return $ret;
        } else {
            self::$CacheConfig = $config;
            return array();
        }
    }

    public static function getStorageAvailableAddress($config) {
        if (self::$UseMem) {
            ShmConfig::configStorage($config);
            $nodes = shm_get_var(self::getShmFd(), self::SHM_STORAGE_KEY);
            $ret = array();
            foreach ((array) $nodes['nodes'] as $node) {
                if (!isset($node['use'])) {//ping没启动
                    break;
                }
                if ($node['use']) {
                    $ret[] = $node[$node['use']]['target'];
                } else {//use=false证明m-s都失效了
                    $ret[] = false;
                }
            }
            return $ret;
        } else {//不用内存，直接用配置
            self::$StorageConfig = $config;
            return array();
        }
    }

}
