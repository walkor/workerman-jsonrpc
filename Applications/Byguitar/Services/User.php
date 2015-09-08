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
        $map['id >'] = 10;

        $db = \Db\Connection::instance();
        $data = $db->read('byguitar_shop')->select('*')->from('bg_manage_menu')->where($map)->queryAll();

        return $data;
    }

}
