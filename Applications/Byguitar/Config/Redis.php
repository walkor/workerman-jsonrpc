<?php
namespace Config;

class Redis{

    public $fav = array('nodes' => array(
                array('master' => "192.168.25.9:6379", 'slave' => "192.168.25.9:6379"),
            ),

            'db' => 2
    );

}