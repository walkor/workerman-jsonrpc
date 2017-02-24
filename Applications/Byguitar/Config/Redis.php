<?php
namespace Config;

class Redis{

    public $default = array('nodes' => array(
                array('master' => "172.16.91.130:6379", 'slave' => "172.16.91.130:6379"),
            ),

            'db' => 2
    );

}
