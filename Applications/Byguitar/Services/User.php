<?php
/**
*  测试
* @author walkor <worker-man@qq.com>
*/
class User{
      
    public static function getInfoByUid($uid)
    {
        return array(
        'uid'    => $uid,
        'name'=> 'test',
        'age'   => 18,
        'sex'    => 'hmm..',
        );
    }

    public static function getEmail($uid)
    {
        return 'worker-man@qq.com';
    }

    public static function TestDb()
    {
        $map = array();
        $map['id >'] = 33;

        $db = \Db\Connection::instance();
        $data = $db->read('byguitar_shop')->select('*')->from('bg_manage_menu')->where($map)->queryAll();

        return $data;
    }

    public static function TestRedis()
    {
        $redis = \Redis\RedisStorage::getInstance();
        
        $in_str = 'test_redis';
        $redis->set('test_key',$in_str,10);

        $out_str = $redis->get('test_key');
        return $out_str;        

    }

}
