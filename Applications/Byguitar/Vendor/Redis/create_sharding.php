<?php

for ($i = 0; $i <= 31; $i++) {
    $pid = pcntl_fork();
    if ($pid === 0) {
        exec("php ShardingRedis.php $i");
        die;
    }
}

