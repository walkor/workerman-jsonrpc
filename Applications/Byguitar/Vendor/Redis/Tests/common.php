<?php
define('ROOT_PATH', __DIR__.'/../');
require ROOT_PATH.'Vendor/Bootstrap/Autoloader.php';
Bootstrap\Autoloader::instance()->addRoot(ROOT_PATH)->addRoot(ROOT_PATH.'../')->addRoot(__DIR__.'/')->init();

$owl_context['trace_id'] = 'redis-unittest-xxx';
$owl_context['parent_id'] = 'redis-unittest-xxx';
$owl_context['uuid'] = 'redis-unittest-xxx-1';