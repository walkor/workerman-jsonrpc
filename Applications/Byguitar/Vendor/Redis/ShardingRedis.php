<?php

/*
 * 切割数据的脚本
 * 使用说明:
 * 1.配置好数据源，例如本demon用的是192.168.8.231,端口6391.
 * 2.配置集群所有机器的ip和端口,$config,向王偉要
 * 3.为了提高效率,将keys(所有的key生成的文件，朝dba王偉要)文件切割成32份,然后启动32个进程，启动进程可以用create_sharding.php脚本
 *   切割文件的shell脚本:
 *   nub=`cat keys|wc -l`
  avg=`expr $nub / 31`
  split -l $avg keys
  会生成demon中的$keyfile中的32个文件。
 * 4.配置好要进行迁移的数据的key前缀。demon中得$KBkeys.
 *  
 */

define("TMP_STRING", 1);
define("TMP_SET", 2);
define("TMP_LISTS", 3);
define("TMP_ZSET", 4);
define("TMP_HASH", 5);

include __DIR__ . '/redis/RedisStorage.class.php';
$config = array(//要求的配置格式。
    'nodes' => array(
        array('master' => "192.168.8.230:27000", 'slave' => "192.168.8.231:27000"),
        array('master' => "192.168.8.230:27001", 'slave' => "192.168.8.231:27001"),
        array('master' => "192.168.8.230:27002", 'slave' => "192.168.8.231:27002"),
        array('master' => "192.168.8.230:27003", 'slave' => "192.168.8.231:27003"),
        array('master' => "192.168.8.230:27004", 'slave' => "192.168.8.231:27004"),
        array('master' => "192.168.8.232:27005", 'slave' => "192.168.8.231:27005"),
        array('master' => "192.168.8.232:27006", 'slave' => "192.168.8.231:27006"),
        array('master' => "192.168.8.232:27007", 'slave' => "192.168.8.231:27007"),
        array('master' => "192.168.8.232:27008", 'slave' => "192.168.8.231:27008"),
        array('master' => "192.168.8.232:27009", 'slave' => "192.168.8.231:27009"),
    ),
    'db' => 0
);
RedisStorage::config($config);

$redis = new Redis();
$redis->connect("192.168.8.231", "6379", 1); //数据源的ip端口
$redis->select(6); //数据源用的的db


$keyfile = array(
    "xaa",
    "xab",
    "xac",
    "xad",
    "xae",
    "xaf",
    "xag",
    "xah",
    "xai",
    "xaj",
    "xak",
    "xal",
    "xam",
    "xan",
    "xao",
    "xap",
    "xaq",
    "xar",
    "xas",
    "xat",
    "xau",
    "xav",
    "xaw",
    "xax",
    "xay",
    "xaz",
    "xba",
    "xbb",
    "xbc",
    "xbd",
    "xbe",
    "xbf",
);

$KBkeys = array(
    "all_buyer_distribution",
    "boms_relations",
    "center_for_brand",
    "center_for_category",
    "center_for_product",
    "cover_choice_List",
    "daily_follow_stats",
    "daily_follow_uids",
    "daily_unfollow_uids",
    "day_keyword",
    "delay_insert_user_product_report",
    "DisposalDataIncremental",
    "each_day_keword_count",
    "event_sys_last_time",
    "how_hot_brand_list",
    "ip_black_lists",
    "Koubei_bayesian_word",
    "koubei_comment_count",
    "koubei_event_center_message_log",
    "koubei_group",
    "koubei_system",
    "Koubei_user_data_check",
    "koubei_user_event",
    "Koubei_User_Latest_View",
    "last_view_product",
    "luckybox",
    "moved",
    "product_blocked_set_report_list",
    "product_category_avg_score",
    "product_statis_users_count",
    "product_with_uid",
    "random_valuable",
    "report",
    "user_attention",
    "user_daren_attention",
    "user_daren_fans",
    "user_fans",
    "user_follow",
    "user_reports_image_info",
    "user_normal",
    "user_reports_image_info",
    "virtual_bundle_set",
    "thumb_file",
    "select_koubei_data",
    "site_map_cache",
); //口碑的key
$index = $argv[1];
//if (!isset($keyfile[$index])) {//echo none之後停止創建進程
//    echo "none\n";
//    $redis->set("sharding_redis_file_count",0);
//    die;
//} else {
//    $redis->incr("sharding_redis_file_count");
//}
$fd = fopen($keyfile[$index], "r");
while (!feof($fd)) {
    $key = trim(fgets($fd));
    foreach ($KBkeys as $KBkey) {
        $len = strlen($KBkey);
        if (strcasecmp($KBkey, substr($key, 0, $len)) === 0) {
            getSet($key);
        }
    }
}

function getSet($key) {
    global $redis;
    try {
        $type = $redis->type($key);
    } catch (Exception $exc) {
        usleep(500);
        $type = $redis->type($key);
    }
    switch ($type) {
        case TMP_STRING:
            getSetString($key);
            break;
        case TMP_SET:
            getSetSet($key);
            break;
        case TMP_LISTS:
            getSetList($key);
            break;
        case TMP_ZSET:
            getSetZSet($key);
            break;
        case TMP_HASH:
            getSetHash($key);
            break;
        default:
            return;
    }
}

function getSetString($key) {
    global $redis;
    try {
        $instance = RedisStorage::getInstance(); //只导存储的
        $type = $instance->type($key);
        if (!empty($type)) {//keys重复兼容
            return;
        }
        $data = $redis->get($key);
        $instance->set($key, $data);
        $data2 = $instance->get($key);
        if ($data !== $data2) {
            redis_log($key);
        } else {
            $redis->incr("sharding_count");
        }
    } catch (Exception $exc) {
        usleep(500);
        redis_log($key);
    }
}

function getSetSet($key) {
    global $redis;
    try {
        $instance = RedisStorage::getInstance(); //只导存储的
        $type = $instance->type($key);
        if (!empty($type)) {//keys重复兼容
            return;
        }
        $data = $redis->sMembers($key);
        foreach ($data as $value) {
            $instance->sadd($key, $value);
        }
        $size1 = $redis->ssize($key);
        $size2 = $instance->ssize($key);
        if ($size1 !== $size2) {
            redis_log($key);
        } else {
            $redis->incr("sharding_count");
        }
    } catch (Exception $exc) {
        usleep(500);
        redis_log($key);
    }
}

function getSetList($key) {
    global $redis;
    try {
        $instance = RedisStorage::getInstance(); //只导存储的
        $type = $instance->type($key);
        if (!empty($type)) {//keys重复兼容
            return;
        }
        $size = $redis->lSize($key);
        for ($i = 0; $i <= intval($size / 100); $i++) {//超大list的兼容
            $data = $redis->lRange($key, $i * 100, ($i + 1) * 100 - 1);
            foreach ($data as $value) {
                $instance->rPush($key, $value);
            }
            unset($data);
            usleep(100);
        }
        $size2 = $instance->lSize($key);
        if ($size !== $size2) {
            redis_log($key);
        } else {
            $redis->incr("sharding_count");
        }
    } catch (Exception $exc) {
        usleep(500);
        redis_log($key);
    }
}

function getSetZSet($key) {
    global $redis;
    try {
        $instance = RedisStorage::getInstance(); //只导存储的
        $type = $instance->type($key);
        if (!empty($type)) {//keys重复兼容
            return;
        }
        $data = $redis->zRange($key, 0, -1, true);
        foreach ($data as $value => $score) {
            $instance->zadd($key, $score, $value);
        }
        $size1 = $redis->zsize($key);
        $size2 = $instance->zsize($key);
        if ($size1 !== $size2) {
            redis_log($key);
        } else {
            $redis->incr("sharding_count");
        }
    } catch (Exception $exc) {
        usleep(500);
        redis_log($key);
    }
}

function getSetHash($key) {
    global $redis;
    try {
        $instance = RedisStorage::getInstance(); //只导存储的
        $type = $instance->type($key);
        if (!empty($type)) {//keys重复兼容
            return;
        }
        $data = $redis->hGetAll($key);
        $instance->hMset($key, $data);
        $size1 = $redis->hlen($key);
        $size2 = $instance->hlen($key);
        if ($size2 !== $size1) {
            redis_log($key);
        } else {
            $redis->incr("sharding_count");
        }
    } catch (Exception $exc) {
        usleep(500);
        redis_log($key);
    }
}

function redis_log($key) {
    $instance = RedisStorage::getInstance(); //只导存储的
    $instance->delete($key);
    global $index;
    file_put_contents("redis_log" . $index, $key . "\n", FILE_APPEND);
    getSet($key);
}
?>
