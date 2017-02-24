<?php

namespace Db;

use \PDO as PDO;

/**
 * mysql only
 * @todo More detailed SqlLog
 */
class Connection {

    /**
     * instance of the DbConnection
     * @var \Db\DbConnection
     */
    protected static $instance;
    protected static $configs;
    protected $currentCfgName = 'default';
    protected static $writeConnections = array();
    protected static $readConnections = array();
    protected $usePool = true;

    /**
     * 进程中是否有事务存在.
     * @var bool
     */
    protected static $inTrans = false;

    /**
     * 533 连接失败; 57P 服务端主动断开连接; HYT 连接超时; IM0 驱动错误; 080 连接出错;
     *
     * @var array 非查询操作错误代码头三位字符.
     */
    protected static $nonQueryErrorCodeHeaders = array('533' => null, '57P' => null, 'HYT' => null, 'IM0' => null, '080' => null);

    /**
     * Established connection.
     *
     * @var \Pdo
     */
    protected $connection;

    /**
     * If directly return query result from page caches. Use noCache() method to change this value.
     *
     * @var boolean
     */
    protected $withCache = true;

    /**
     * Cached results of queries in the same page/request.
     *
     * @var array
     */
    protected $cachedPageQueries = array();

    /**
     * If in global transaction. refers to {@link self::beginTransaction}
     *
     * @var Boolean
     */
    protected $inGlobalTransaction = false;
    protected $queryBeginTime;
    protected $queryEndTime;
    protected $connectionCfg = array();
    protected $allowRealExec = true;
    protected $allowSaveToNonExistingPk = false;
    protected $allowGuessConditionOperator = true; //null: allow but warning.      false: not allowed and throw exception.     true: allowed
    protected $autoCloseLastStatement = false;
    protected $lastSql;
    protected $lastStmt;
    protected $select_sql_top;
    protected $select_sql_columns;
    protected $select_sql_from_where;
    protected $select_sql_group_having;
    protected $select_sql_order_limit;
    protected $memoryUsageBeforeFetch;
    protected $memoryUsageAfterFetch;

    const UPDATE_NORMAL = 0;
    const UPDATE_IGNORE = 1;
    const INSERT_ON_DUPLICATE_UPDATE = 'ondup_update';
    const INSERT_ON_DUPLICATE_UPDATE_BUT_SKIP = 'ondup_exclude';
    const INSERT_ON_DUPLICATE_IGNORE = 'ondup_ignore';

    protected function __construct($dsn = null, $username = null, $passwd = null, $options = array()) {
        if (defined('JM_PHP_CONN_POOL_ON')) {
            $this->usePool = (bool) JM_PHP_CONN_POOL_ON;
        }
        if (!extension_loaded('connect_pool')) {
            $this->usePool = false;
        }
        if (!self::$configs) {
            if (!class_exists('\Config\Db')) {
                $this->throwException('Neither configurations are set nor  Config\Db are found!');
            }
            self::$configs = (array) new \Config\Db;
        }
        if (!is_null($dsn)) {
            $this->connect($dsn, $username, $passwd, $options);
        }
    }

    public function __destruct() {
        $this->lastStmt = null;
    }

    /**
     * 将配置中的DSN(负载均衡会配置多组ip:host、权重等)转成多条标准的pdo dsn格式。原始格式如："mysql:dbname=tuanmei_operation;host=192.168.20.71:9001:1,192.168.20.72:9001:1"
     * @param $cfg 数据库的原始配置.
     * @return array 一个或多个包含标准dsn配置 如: array(0=>array('dsn'=>'dbname=testdb;host=127.0.0.1;port=3306', 'weigth'=>1), 1 => array('dsn'=>'dbname=testdb;host=127.0.0.2;port=3306', 'weigth'=>2));除以上信息外还会保留原来的额外配置.
     * @throws \Db\Exception
     */
    public static function parseCfg($cfg)
    {
        if(!isset($cfg['dsn']))
        {// 没有配dsn，可能已经手动拆分好了各节点.
            return $cfg;
        }
        $dsn = $cfg['dsn'];
        unset($cfg['dsn']);
        $mainPortions = explode(':', $dsn, 2);
        $driverType = $mainPortions[0];
        $dbPortions = explode(';', $mainPortions[1]);
        foreach($dbPortions as $dbPortion)
        {
            $tempPortionPair = explode('=', $dbPortion);
            switch($tempPortionPair[0])
            {
                case 'dbname' :
                    $dbname = $tempPortionPair[1];
                    continue;
                case 'host' :
                    $hostRaw = $tempPortionPair[1];
                    continue;
                case 'port' :
                    $defaultPort = $tempPortionPair[1];
            }
        }

        if(!isset($dbname) || !isset($hostRaw))
        {
            throw new Exception('Invalid dsn to parse!');
        }
        $hosts = explode(',', $hostRaw);
        $parsed = array();
        foreach($hosts as  $host)
        {
            $hostPortions = explode(':', $host);
            $parsedDsn = $driverType.':dbname='.$dbname.';host='.$hostPortions[0].';';
            $port = isset($hostPortions[1]) && !empty($hostPortions[1]) ? $hostPortions[1] :
                (isset($defaultPort) ? $defaultPort : 3306);
            $parsedDsn .= 'port='.$port;
            $weight = isset($hostPortions[2]) ? $hostPortions[2] : 1;
            $parsed[] = array_merge(array('dsn' => $parsedDsn, 'weight' => $weight), $cfg);
        }
        return $parsed;
    }

    /**
     * Set or get configs for the lib.
     *
     * @param string $config
     * @return boolean
     */
    public static function config($config = null) {
        if (is_null($config)) {
            return static::$configs;
        }
        static::$configs = $config;
        return true;
    }

    /**
     * Get the current connection object. DO NOT heavily use this method in a single script.
     *
     * @return \PDO
     */
    public function getConn() {
        if (!$this->usePool) {
            $this->reConnect();
        }
        return $this->connection;
    }

    public function lastConnectionError()
    {
        return $this->connection->errorInfo();
    }

    /**
     * Close all connections.
     *
     * @return boolean
     */
    public function closeAll() {
        foreach (static::$readConnections as $k => $v) {
            if ($v) {
                $v->close();
            }
            $v = null;
            unset(static::$readConnections[$k]);
        }
        foreach (static::$writeConnections as $k => $v) {
            if ($v) {
                $v->close();
            }
            $v = null;
            unset(static::$writeConnections[$k]);
        }
        return true;
    }

    /**
     * Close current connection of this instance.
     */
    public function close() {
        // 这里用pdo底层的方法判断，有可能链接已经在网络层被断开了.
        if ($this->connection->inTransaction()) {
            // 有由于未知原因事务未被完成.
            trigger_error(new Exception('There is still active transaction'), E_USER_WARNING);
            $this->rollback();
        }
        $this->release(true);
        $this->connection = null;
        $this->lastStmt = null;
        return true;
    }

    public function inTrans() {
        return $this::$inTrans;
    }

    /**
     * Clear all connection query caches of a request page.
     */
    public function clearPageCaches() {
        foreach (static::$readConnections as $link) {
            $link->destroyPageCache();
        }
        foreach (static::$writeConnections as $link) {
            $link->destroyPageCache();
        }
        return true;
    }

    /**
     * Clean the query caches of the current connection.
     *
     * @return array
     */
    public function destroyPageCache() {
        return $this->cachedPageQueries = array();
    }

    /**
     * get a instance of \Db\Connection
     * @return static
     */
    public static function instance() {
        if (!static::$instance) {
            static::$instance = new static();
        }
        return static::$instance;
    }

    /**
     * set config name for the current instance. after this all read()/write() method will use this connection of this config.<br />
     * Example:<br />
     * <pre>
     *     <code>
     *     $db = new \Db\Connection();
     *     $db->read('CfgNotOfDefault')->query($sql);
     *     $db->write('CfgNotOfDefault')->query($sql);
     *     //Equals to the following.
     *     $db->setCfg('CfgNotOfDefault');
     *     $db->read()->query($sql);
     *     $db->write()->query($sql);
     *     </code>
     * </pre>
     * @param string $name
     */
    public function setCfgName($name) {
        $this->currentCfgName = $name;
    }

    /**
     *
     * @param string $name
     * @throws Exception
     * @return \Db\Connection
     */
    public function write($name = 'default') {
        if ($name == 'default' && func_num_args() == 0 && $this->currentCfgName) {
            $name = $this->currentCfgName;
        }
        if (empty(self::$writeConnections[$name]) && !$this->addWriteConnection($name)) {
            $this->throwException('No available write connections. Please use addWriteConnection to initialize  first', 42001);
        }
        return self::$writeConnections[$name];
    }

    /**
     * 是否是查询执行出错，而非连接出错，服务端异常等。
     *
     * @param $errorCode 错误代码(ansi sql error code, 参考: php-src/ext/pdo/pdo_sqlstate.c)
     * @return bool
     */
    public function isQueryError($errorCode) {
        $codeHeader = substr($errorCode, 0, 3);
        return !isset($this::$nonQueryErrorCodeHeaders[$codeHeader]);
    }

    /**
     * 在查询失败后修复连接不可关闭的pdo bug.
     *
     * @param $errorCode 错误代码(ansi sql error code, 参考: php-src/ext/pdo/pdo_sqlstate.c)
     * @return bool
     */
    protected function fixConnectionAfterQueryError($errorCode) {
        if ($this->isQueryError($errorCode)) {
            return (bool) $this->connection->query('select 1');
        }
        return true;
    }

    /**
     *
     * @param string $name
     * @throws Exception
     * @return \Db\Connection
     * @todo connection name select
     */
    public function read($name = 'default') {
        if ($name == 'default' && func_num_args() == 0 && $this->currentCfgName) {
            $name = $this->currentCfgName;
        }
        if (empty(self::$readConnections[$name]) && !$this->addReadConnection($name)) {
            $this->throwException('No available read connections. Please use addReadConnection to initialize  first', 42001);
        }
        return self::$readConnections[$name];
    }

    /**
     * initialize read connections
     *
     * @param string $name
     * @return \Db\Connection
     */
    public function addReadConnection($name = 'default') {
        if (isset(self::$configs['read'][$name])) {
            // 标准化为可用于负载均衡过程选取访问节点的格式.
            $cfg = $this->parseCfg(self::$configs['read'][$name]);
            //客户端负载模式, 选取访问节点
            $cfg = $this->getOneSlave($cfg);
            $connection = new self($cfg ['dsn'], $cfg ['user'], $cfg ['password'], $cfg ['options']);
            $connection->connectionCfg = $cfg;
            self::$readConnections [$name] = $connection;
            return self::$readConnections [$name];
        } else {
            $this->throwException('Read configuration of "' . $name . '" is not found.', 42003);
        }
    }

    /**
     * initialize write connections
     *
     * @param string $name
     * @return \Db\Connection
     */
    public function addWriteConnection($name = 'default') {
        if (isset(self::$configs['write'][$name])) {
            $cfg = self::$configs['write'][$name];
            $connection = new self($cfg ['dsn'], $cfg ['user'], $cfg ['password'], $cfg ['options']);
            $connection->connectionCfg = $cfg;
            self::$writeConnections [$name] = $connection;
            return self::$writeConnections [$name];
        } else {
            $this->throwException('Write configuration of "' . $name . '" is not found.', 42003);
        }
    }

    /**
     *
     * @param string $dsn
     * @param string $user
     * @param string $password
     * @param array $options
     * @return \Db\Connection
     */
    public function connect($dsn, $user = null, $password = null, $options = array()) {
        if (is_array($dsn)) {
            extract($dsn);
        }
        if ($this->connection) {
            return $this->connection;
        } else {
            try {
                if ($this->usePool) {
                    $this->connection = new \pdo_connect_pool($dsn, $user, $password, $options);
                    if ($this->connection->inTransaction()) {// 当新获取的连接带有事务时,有可能是上次使用事务的进程被强行退出.
                        trigger_error('There are still active transaction in connection that retrieved from connect pool, now rollback before continue.', E_USER_WARNING);
                        $this->connection->rollBack();
                    }
                } else {
                    $this->connection = new \PDO($dsn, $user, $password, $options);
                }
            } catch (\PDOException $ex) {
                if (strstr($ex->getMessage(), "connect to pool_server fail")) {//端口连不上
                    $this->connection = new \PDO($dsn, $user, $password, $options);
                    $this->usePool = false;
                } else {
                    $this->throwException($ex);
                }
            }
        }
        return $this;
    }

    protected function reConnect() {
        $this->close();
        return $this->connect($this->connectionCfg);
    }

    public function insert($table, $params, $onDup = null) {
        $columns = '';
        $values = '';
        foreach ($params as $column => $value) {
            $columns .= $this->quoteObj($column) . ',';
            $values .= is_null($value) ? "NULL," : ($this->quote($value) . ',');
        }

        $columns = substr($columns, 0, strlen($columns) - 1);
        $values = substr($values, 0, strlen($values) - 1);

        $sql_part_ignore = '';
        $sql_part_on_dup = '';

        if (empty($onDup)) {
            //do nothing, use the default behavior
        } else if ($onDup == self::INSERT_ON_DUPLICATE_IGNORE) {
            $sql_part_ignore = 'IGNORE';
        } else if ($onDup == self::INSERT_ON_DUPLICATE_UPDATE) {
            if (func_num_args() >= 4)
                $update_params = func_get_arg(3);
            else
                $update_params = $params;

            $updates = array();
            foreach ($update_params as $column => $value) {
                if (is_int($column))
                    $updates[] = "$value";
                else
                    $updates[] = $this->quoteObj($column) . "=" . (is_null($value) ? "null" : $this->quote($value));
            }
            if ($updates)
                $sql_part_on_dup = 'ON DUPLICATE KEY UPDATE ' . join(",", $updates);
        }
        else if ($onDup == self::INSERT_ON_DUPLICATE_UPDATE_BUT_SKIP) {
            $noUpdateColumnNames = func_get_arg(3);
            if (!is_array($noUpdateColumnNames))
                $this->throwException('invalid INSERT_ON_DUPLICATE_UPDATE_BUT_SKIP argument');

            $updates = array();
            foreach ($params as $column => $value) {
                if (!in_array($column, $noUpdateColumnNames)) {
                    $column = $this->quoteObj($column);
                    $updates[] = "$column=" . (is_null($value) ? "null" : $this->quote($value));
                }
            }
            $sql_part_on_dup = 'ON DUPLICATE KEY UPDATE ' . join(",", $updates);
        }

        $table = $this->quoteObj($table);
        $sql = "INSERT $sql_part_ignore INTO $table ($columns) VALUES ($values) $sql_part_on_dup";
        $ret = $this->exec($sql, false);

        if ($ret === false) {
            $this->release();
            return false;
        }

        $id = $this->connection->lastInsertId();
        $this->release();
        if ($id)
            return $id;

        return !!$ret;
    }

    public function replace($table, $params) {
        $columns = '';
        $values = '';
        foreach ($params as $column => $value) {
            $columns .= $this->quoteObj($column) . ',';
            $values .= is_null($value) ? "NULL," : ($this->quote($value) . ',');
        }

        $columns = substr($columns, 0, strlen($columns) - 1);
        $values = substr($values, 0, strlen($values) - 1);

        $table = $this->quoteObj($table);
        $sql = "REPLACE INTO $table ($columns) VALUES ($values)";
        $ret = $this->exec($sql);

        if ($ret === false) {
            $this->release();
            return false;
        }

        $id = $this->connection->lastInsertId();
        $this->release();
        if ($id)
            return $id;

        return $ret;
    }

    public function quote($data, $paramType = PDO::PARAM_STR) {
        if (is_array($data) || is_object($data)) {
            $return = array();
            foreach ($data as $k => $v) {
                $return [$k] = $this->quote($v);
            }
            return $return;
        } else {
            $data = $this->connection->quote($data, $paramType);
            if (false === $data)
                $data = "''";
            return $data;
        }
    }

    /**
     * quote object names.<br />
     * e.g. as mysql, a table name "user" will be quoted to "`user`", column name "t1.cl1 as haha" will be quoted to "`t1`.`cl1` AS `haha`"
     *
     * @param string|array $objName
     * @todo only mysql is currently supported.
     * @return mixed
     */
    public function quoteObj($objName) {
        if (is_array($objName)) {
            $return = array();
            foreach ($objName as $k => $v) {
                $return[] = $this->quoteObj($v);
            }
            return $return;
        } else {
            $v = trim($objName);
            $v = str_replace('`', '', $v);
            $v = preg_replace('# +AS +| +#i', ' ', $v);
            $v = explode(' ', $v);
            foreach ($v as $k_1 => $v_1) {
                $v_1 = trim($v_1);
                if ($v_1 == '') {
                    unset($v[$k_1]);
                    continue;
                }
                if (strpos($v_1, '.')) {
                    $v_1 = explode('.', $v_1);
                    foreach ($v_1 as $k_2 => $v_2) {
                        $v_1[$k_2] = '`' . trim($v_2) . '`';
                    }
                    $v[$k_1] = implode('.', $v_1);
                } else {
                    $v[$k_1] = '`' . $v_1 . '`';
                }
            }
            $v = implode(' AS ', $v);
            return $v;
        }
    }

    public function throwException($message = null, $code = null, $previous = null) {
        if (is_object($message)) {
            $ex = $message;
        } else {
            if ($this->usePool) {// 连接池暂时还没有提供ＰＤＯ系列的错误信息获取方法.
                $ex = new Exception($message, $code, $previous);
            } else {
                if($this->connection) {
                    $errorInfo = $this->connection->errorInfo();
                    $ex = new Exception($message . ' (DriverCode:' . $errorInfo[1] . ')' . $errorInfo [2], $code, $previous);
                } else {
                    $ex = new Exception($message);
                }

            }
        }
        $this->release();
        throw $ex;
    }

    /**
     * Indicates the next query do not use page caches.
     *
     * @return self
     */
    public function noCache() {
        $this->withCache = false;
        return $this;
    }

    /**
     * By default, results (from select statement) are to be get from page caches. Please use the following syntax to get results from database in every query.
     * E.G.<pre>
     * DbConnection::instance()->read()->noCache()->query('....');
     * </pre>
     * @param string $sql
     * @return PDOStatement
     * @see PDO::query()
     * @throws \Db\Exception
     */
    public function query($sql = null) {
        static $retryCount = 0;

        $withCache = false; //$this->withCache;
        //reset withCache to true in every query, so the next query will use cache again.
        $this->withCache = true;

        if (empty($sql)) {
            $this->lastSql = $this->getSelectSql();  // 不需要trim，拼接函数保证以SELECT开头
        } else {
            $this->lastSql = trim($this->buildSql($sql));
        }
        $sqlCmd = strtoupper(substr($this->lastSql, 0, 6));
        if (in_array($sqlCmd, array('UPDATE', 'DELETE')) && stripos($this->lastSql, 'where') === false) {
            $this->throwException('no WHERE condition in SQL(UPDATE, DELETE) to be executed! please make sure it\'s safe', 42005);
        }

        if ($this->allowRealExec || $sqlCmd == 'SELECT') {
            $cacheKey = md5($this->lastSql);
            if ($withCache && isset($this->cachedPageQueries[$cacheKey])) {
                return $this->cachedPageQueries[$cacheKey];
            }
            $this->queryBeginTime = microtime(true);
            $trace = $this::getExternalCaller();
            //\MNLogger\TraceLogger::instance('trace')->MYSQL_CS($this->connectionCfg['dsn'], $trace['class'] . '::' . $trace['method'], $this->lastSql, $trace['file'] . ':' . $trace['line']);
            $this->memoryUsageBeforeFetch = memory_get_usage();
            try {// 连接池在查询失败时也会抛异常,而pdo在默认情况下会返回false.
                $this->lastStmt = $this->connection->query($this->lastSql);
            } catch (\Exception $queryEx) {
                $this->lastStmt = false;
            }
        } else {
            $this->lastStmt = true;
        }
        $this->queryEndTime = microtime(true);
        $this->logQuery($this->lastSql);
        if (false === $this->lastStmt) {
            // connection broken, retry one time
            if ($retryCount < 1 && $this->needConfirmConnection()) {
                $connectionLost = false;
                if (!$this->usePool) {
                    $errorInfo = $this->connection->errorInfo();
                    $connectionLost = 2006 == $errorInfo[1];
                } else {
                    if (isset($queryEx)) {// 目前只能通过message字符串匹配,不是很严谨.
                        $connectionLost = stripos($queryEx->getMessage(), 'gone away') !== false;
                        trigger_error($queryEx, E_USER_WARNING);
                        if (!$connectionLost) {
                            $this->throwException($queryEx);
                        }
                    } else {
                        if ($errorInfo = $this->connection->errorInfo()) {
                            $errorInfo = $errorInfo[2];
                        } else {
                            $errorInfo = 'Un-handleable connect pool error. sql: ' . $this->lastSql;
                        }
                        $this->throwException(new Exception($errorInfo));
                    }
                }
                if ($connectionLost) {
                    $retryCount += 1;
                    $this->reConnect();
                    $result = $this->query($sql);
                    $retryCount = 0;
                    return $result;
                }
            } else {
                $this->fixConnectionAfterQueryError($errorInfo[0]);
            }
            $retryCount = 0;
            $this->throwException('Query failure.SQL:' . $this->lastSql . '. (' . $errorInfo[2] . ')', 42004);
        }

        if ($withCache && isset($cacheKey)) {
            $this->cachedPageQueries[$cacheKey] = $this->lastStmt;
        }

        if ($this->usePool) {
            if ($this->lastStmt instanceof \pdo_connect_pool_PDOStatement) {
                return $this->lastStmt;
            }
            $this->throwException('Unexpected type of query statement. Expecting "pdo_connect_pool_PDOStatement" but "' . var_export($this->lastStmt, true) . " presents. SQL: " . $this->lastSql, 42010);
        } else {
            if ($this->lastStmt instanceof \PDOStatement) {
                return $this->lastStmt;
            }
            $this->throwException('Unexpected type of query statement. Expecting "PDOStatement" but "' . var_export($this->lastStmt, true) . " presents. SQL: " . $this->lastSql, 42010);
        }
    }

    /**
     * @param string $sql
     * @param bool $releaseConnection 执行完是否立即释放连接.如果立即释放,则可能会影响到进程内后续的连接判断。如: 当要获取到lastInsertId时则应当保持连接，否则有可能是获取到其它查询的返回值。
     * @see PDO::exec()
     * @return mixed
     */
    public function exec($sql = null, $releaseConnection = true) {
        static $retryCount = 0;
        $sqlCmd = strtoupper(substr($sql, 0, 6));
        if (in_array($sqlCmd, array('UPDATE', 'DELETE')) && stripos($sql, 'where') === false) {
            $this->throwException(new Exception('no WHERE condition in SQL(UPDATE, DELETE) to be executed! please make sure it\'s safe', 42005));
        }

        $this->queryBeginTime = microtime(true);
        $trace = $this::getExternalCaller();
        //\MNLogger\TraceLogger::instance('trace')->MYSQL_CS($this->connectionCfg['dsn'], $trace['class'] . '::' . $trace['method'], $this->lastSql, $trace['file'] . ':' . $trace['line']);
        if ($this->allowRealExec) {
            try {
                $re = $this->connection->exec($sql);
            } catch (\Exception $queryEx) {
                $re = false;
            }
        } else {
            $re = true;
        }
        //\MNLogger\TraceLogger::instance('trace')->MYSQL_CR($re === false ? 'EXCEPTION' : 'SUCCESS', 0);
        $this->queryEndTime = microtime(true);
        $this->logQuery($sql);
        if (false === $re) {
            // connection broken, retry one time
            if ($retryCount < 1 && $this->needConfirmConnection()) {
                $connectionLost = false;
                if (!$this->usePool) {
                    $errorInfo = $this->connection->errorInfo();
                    $connectionLost = 2006 == $errorInfo[1];
                } else {
                    if (isset($queryEx)) {// 目前只能通过message字符串匹配,不是很严谨.
                        $connectionLost = stripos($queryEx->getMessage(), 'gone away') !== false;
                    } else {
                        if ($errorInfo = $this->connection->errorInfo()) {
                            $errorInfo = $errorInfo[2];
                        } else {
                            $errorInfo = 'Un-handleable connect pool error. sql: ' . $this->lastSql;
                        }
                        $this->throwException(new Exception($errorInfo));
                    }
                }
                if ($connectionLost) {
                    $retryCount += 1;
                    $this->reConnect();
                    $re = $this->exec($sql);
                    $retryCount = 0;
                    return $re;
                }
            }
            $retryCount = 0;
            $this->throwException('Query failure.SQL:' . $sql . '.(' . $errorInfo[2] . ') ', 42004);
        }
        if ($releaseConnection)
            $this->release();
        return $re;
    }

    /**
     * @param boolean $global If use transaction for all queries.
     *                        If it is set to true, you have to set it to true when "commit" or "rollback" to make all queries effective within it.
     *                        Once in global transaction, any nested in transactions are disabled, and will be included within the global transaction.
     *                        Notice: glboal transaction can not be netsted within any other transactions, it should be stated from the outmost level.
     * @return bool
     */
    public function beginTransaction($global = false) {
        if ($global && $this->connection->inTransaction()) {// Allow one global transaction only.
            $this->connection->rollBack();
            $this::$inTrans = false;
            $this->throwException('You cannot begin global transaction at this moment. There are active transactions or GlobalTransaction has already started !', 42101);
        } else if (!$global && $this->inGlobalTransaction) {// If global transaction started, then ignore all normal transactions(just not start them).
            return true;
        } else if ($global) {// Start global transaction.
            $this->inGlobalTransaction = true;
        }

        if (!$this->usePool && !$this->connection->inTransaction()) {
        // Re-connect before begin a transaction. If inTransaction then skip this step to avoid breaking nested transactions.
            $this->reConnect();
        }
        return $this::$inTrans = $this->connection->beginTransaction(); //在事务里面持有其他连接，这个连接也不释放（即:一次release这个进程持有的所有连接都释放了）
    }

    /**
     *
     * @param boolean $global if commit the global transaction.
     *
     * @return boolean
     */
    public function commit($global = false) {
        if ($this->inGlobalTransaction && !$global) {// Prevent committing a global transaction unexpectedly in a normal transaction.
            return true;
        } else {// Ready to commit the global transaction.
            $this->inGlobalTransaction = false;
        }
        $ret = $this->connection->commit();
        $this::$inTrans = false;
        if (!$ret) {
            $errorInfo = $this->connection->errorCode();
            $this->fixConnectionAfterQueryError($errorInfo[0]);
            trigger_error(new Exception($errorInfo[2]), E_USER_WARNING);
        }
        $this->release();
        return $ret;
    }

    /**
     *
     * @param boolean $global if rollback the global transaction.
     *
     * @return boolean
     */
    public function rollback($global = false) {
        if ($this->inGlobalTransaction && !$global) {// Prevent rollback a global transaction unexpectedly in a normal transaction.
            $this::$inTrans = true;
            return true;
        } else {// Ready to rollback the global transaction.
            $this->inGlobalTransaction = false;
        }
        $ret = $this->connection->rollBack();
        $this::$inTrans = false;
        $this->release();
        return $ret;
    }

    /**
     * Check if confirmation of connection is needed by setting "confirm_link" of configuration  to true.
     * This is mostly used in Daemons which use long connections.
     *
     * @return boolean
     */
    public function needConfirmConnection() {
        if (isset($this->connectionCfg['confirm_link']) || $this->connectionCfg['confirm_link'] !== false) {
            return true;
        }
        return false;
    }

    public function buildWhere($condition = array(), $logic = 'AND') {
        $s = $this->buildCondition($condition, $logic);
        if ($s)
            $s = ' WHERE ' . $s;
        return $s;
    }

    public function buildCondition($condition = array(), $logic = 'AND') {
        if (!is_array($condition)) {
            if (is_string($condition)) {
                //forbid to use a CONSTANT as condition
                $count = preg_match('#\>|\<|\=| #', $condition, $logic);
                if (!$count) {
                    $this->throwException('bad sql condition: must be a valid sql condition');
                }
                $condition = explode($logic[0], $condition);
                if (!is_numeric($condition[0])) {
                    $condition[0] = $this->quoteObj($condition[0]);
                }
                $condition = implode($logic[0], $condition);
                return $condition;
            }

            $this->throwException('bad sql condition: ' . gettype($condition));
        }
        $logic = strtoupper($logic);
        $content = null;
        foreach ($condition as $k => $v) {
            $v_str = null;
            $v_connect = '';

            if (is_int($k)) {
                //default logic is always 'AND'
                if ($content)
                    $content .= $logic . ' (' . $this->buildCondition($v) . ') ';
                else
                    $content = '(' . $this->buildCondition($v) . ') ';
                continue;
            }

            $k = trim($k);

            $maybe_logic = strtoupper($k);
            if (in_array($maybe_logic, array('AND', 'OR'))) {
                if ($content)
                    $content .= $logic . ' (' . $this->buildCondition($v, $maybe_logic) . ') ';
                else
                    $content = '(' . $this->buildCondition($v, $maybe_logic) . ') ';
                continue;
            }

            $k_upper = strtoupper($k);
            //the order is important, longer fist, to make the first break correct.
            $maybe_connectors = array('>=', '<=', '<>', '!=', '>', '<', '=',
                ' NOT BETWEEN', ' BETWEEN', 'NOT LIKE', ' LIKE', ' IS NOT', ' NOT IN', ' IS', ' IN');
            foreach ($maybe_connectors as $maybe_connector) {
                $l = strlen($maybe_connector);
                if (substr($k_upper, -$l) == $maybe_connector) {
                    $k = trim(substr($k, 0, -$l));
                    $v_connect = $maybe_connector;
                    break;
                }
            }
            if (is_null($v)) {
                $v_str = ' NULL';
                if ($v_connect == '') {
                    $v_connect = 'IS';
                }
            } else if (is_array($v)) {
                if ($v_connect == ' BETWEEN') {
                    $v_str = $this->quote($v[0]) . ' AND ' . $this->quote($v[1]);
                } else if (is_array($v) && !empty($v)) {
                    // 'key' => array(v1, v2)
                    $v_str = null;
                    foreach ($v AS $one) {
                        if (is_array($one)) {
                            // (a,b) in ( (c, d), (e, f) )
                            $sub_items = '';
                            foreach ($one as $sub_value) {
                                $sub_items .= ',' . $this->quote($sub_value);
                            }
                            $v_str .= ',(' . substr($sub_items, 1) . ')';
                        } else {
                            $v_str .= ',' . $this->quote($one);
                        }
                    }
                    $v_str = '(' . substr($v_str, 1) . ')';
                    if (empty($v_connect)) {
                        if ($this->allowGuessConditionOperator === null || $this->allowGuessConditionOperator === true) {
                            if ($this->allowGuessConditionOperator === null)
                                \Log\Handler::instance()->log("guessing condition operator is not allowed: use '$k IN'=>array(...)", array('type' => E_WARNING));

                            $v_connect = 'IN';
                        } else
                            $this->throwException("guessing condition operator is not allowed: use '$k IN'=>array(...)");
                    }
                }
                else if (empty($v)) {
                    // 'key' => array()
                    $v_str = $k;
                    $v_connect = '<>';
                }
            } else {
                $v_str = $this->quote($v);
            }

            if (empty($v_connect))
                $v_connect = '=';

            $quoted_k = $this->quoteObj($k);
            if ($content)
                $content .= " $logic ( $quoted_k $v_connect $v_str ) ";
            else
                $content = " ($quoted_k $v_connect $v_str) ";
        }

        return $content;
    }

    protected function buildSql($sql) {
        $realSql = '';
        if (is_string($sql))
            return $sql;
        if (is_array($sql)) {
            $realSql = '';
            foreach ($sql as $k => $v) {
                if (is_int($k))
                    $realSql .= $v . " ";
                else if ($k == 'where' || $k == 'WHERE')
                    $realSql .= " WHERE " . $this->buildCondition($v) . " ";
                else
                    \Log\Handler::instance()->log('unknown key("' . $k . '") in sql.');
            }
        }
        return $realSql;
    }

    public function setAllowRealExec($v) {
        $this->allowRealExec = $v;
    }

    /**
     * 只有在主键不是自增id的时候，调用saveWithoutNull的时候才需要allowSaveToNonExistingPk
     */
    public function setAllowSaveToNonExistingPk($v) {
        $this->allowSaveToNonExistingPk = $v;
    }

    /**
     * 是否允许条件构造的时候，自动推导操作符。例如：是否允许 'a'=>array(1,2) 推导为  a IN (1,2)
     * 如果允许，则对输入数据进行过滤，确保需要提交一个数据的地方，不要被提交上一个数组。
     *
     * @param $v   null: allow but log a warning.      false: not allowed and throw exception.     true: allowed
     */
    public function setAllowGuessConditionOperator($v) {
        $this->allowGuessConditionOperator = $v;
    }

    public function getLastSql() {
        return $this->lastSql;
    }

    public function getSelectSql() {
        return "SELECT {$this->select_sql_top} {$this->select_sql_columns} {$this->select_sql_from_where} {$this->select_sql_group_having} {$this->select_sql_order_limit}";
    }

    /**
     * @param string $columns
     * @return \Db\Connection
     */
    public function select($columns = '*') {
        $this->select_sql_top = '';
        $this->select_sql_columns = $columns;
        $this->select_sql_from_where = '';
        $this->select_sql_group_having = '';
        $this->select_sql_order_limit = '';
        return $this;
    }

    /**
     * @param $n
     * @return \Db\Connection
     */
    public function top($n) {
        $n = intval($n);
        $this->select_sql_top = "TOP $n";
    }

    /**
     * @param $table
     * @return \Db\Connection
     */
    public function from($table) {
        $table = $this->quoteObj($table);
        $this->select_sql_from_where .= " FROM $table ";
        return $this;
    }

    /**
     * @param array|string $cond
     * @return \Db\Connection
     */
    public function where($cond = array()) {
        $cond = $this->buildCondition($cond);
        $this->select_sql_from_where .= $cond ? " WHERE $cond " : '';
        return $this;
    }

    protected function joinInternal($join, $table, $cond) {
        $table = $this->quoteObj($table);
        $this->select_sql_from_where .= " $join $table ";
        if (is_string($cond) && (strpos($cond, '=') === false && strpos($cond, '<') === false && strpos($cond, '>') === false)) {
            $column = $this->quoteObj($cond);
            $this->select_sql_from_where .= " USING ($column) ";
        } else {
            $cond = $this->buildCondition($cond);
            $this->select_sql_from_where .= " ON $cond ";
        }
        return $this;
    }

    /**
     * @param $table
     * @param $cond
     * @return \Db\Connection
     */
    public function join($table, $cond) {
        return $this->joinInternal('JOIN', $table, $cond);
    }

    /**
     * @param $table
     * @param $cond
     * @return \Db\Connection
     */
    public function leftJoin($table, $cond) {
        return $this->joinInternal('LEFT JOIN', $table, $cond);
    }

    /**
     * @param $table
     * @param $cond
     * @return \Db\Connection
     */
    public function rightJoin($table, $cond) {
        return $this->joinInternal('RIGHT JOIN', $table, $cond);
    }

    public function update($table, $params, $cond, $options = 0, $order_by_limit = '') {
        if (empty($params))
            return false;

        if (is_string($params)) {
            $update_str = $params;
        } else {
            $update_str = '';

            foreach ($params as $column => $value) {
                if (is_int($column)) {
                    $update_str .= "$value,";
                } else {
                    $column = $this->quoteObj($column);
                    $value = is_null($value) ? 'NULL' : $this->quote($value);
                    $update_str .= "$column=$value,";
                }
            }
            $update_str = substr($update_str, 0, strlen($update_str) - 1);
        }

        $table = $this->quoteObj($table);
        if (is_numeric($cond))
            $cond = $this->quoteObj('id') . "='$cond'";
        else
            $cond = $this->buildCondition($cond);
        $sql = "UPDATE ";
        if ($options == self::UPDATE_IGNORE)
            $sql .= " IGNORE ";
        $sql .= " $table SET $update_str WHERE $cond $order_by_limit";
        $ret = $this->exec($sql);
        return $ret;
    }

    public function delete($table, $cond) {
        $table = $this->quoteObj($table);
        $cond = $this->buildCondition($cond);
        $sql = "DELETE FROM {$table} WHERE $cond";
        $ret = $this->exec($sql);
        return $ret;
    }

    /**
     * @param $group
     * @return \Db\Connection
     */
    public function group($group) {
        $this->select_sql_group_having .= " GROUP BY $group ";
        return $this;
    }

    /**
     * @param $having
     * @return \Db\Connection
     */
    public function having($cond) {
        $cond = $this->buildCondition($cond);
        $this->select_sql_group_having .= " HAVING $cond ";
        return $this;
    }

    /**
     * @param $order
     * @return \Db\Connection
     */
    public function order($order) {
        $this->select_sql_order_limit .= " ORDER BY $order ";
        return $this;
    }

    public function isDriver($name) {
        $driver = $this->getAttribute(PDO::ATTR_DRIVER_NAME);
        if (is_array($name))
            return in_array($driver, $name);
        return $driver == $name;
    }

    public function queryScalar($sql = null, $default = null) {
        $stmt = $this->query($sql);
        $v = $stmt->fetchColumn(0);
        $this->release();
        $this->memoryUsageAfterFetch = memory_get_usage();
        //\MNLogger\TraceLogger::instance('trace')->MYSQL_CR($this->lastStmt ? 'SUCCESS' : 'EXCEPTION', $this->memoryUsageAfterFetch - $this->memoryUsageBeforeFetch);
        if ($v !== false)
            return $v;
        return $default;
    }

    public function querySimple($sql = null, $default = null) {
        return $this->queryScalar($sql, $default);
    }

    /**
     * @param string|null $sql
     * @return array
     */
    public function queryRow($sql = null) {
        $stmt = $this->query($sql);
        $data = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->release();
        $this->memoryUsageAfterFetch = memory_get_usage();
        //\MNLogger\TraceLogger::instance('trace')->MYSQL_CR($this->lastStmt ? 'SUCCESS' : 'EXCEPTION', $this->memoryUsageAfterFetch - $this->memoryUsageBeforeFetch);
        return $data;
    }

    /**
     * @param string|null $sql
     * @return array
     */
    public function queryColumn($sql = null) {
        $stmt = $this->query($sql);
        $data = $stmt->fetchAll(PDO::FETCH_COLUMN, 0);
        $this->release();
        $this->memoryUsageAfterFetch = memory_get_usage();
        //\MNLogger\TraceLogger::instance('trace')->MYSQL_CR($this->lastStmt ? 'SUCCESS' : 'EXCEPTION', $this->memoryUsageAfterFetch - $this->memoryUsageBeforeFetch);
        return $data;
    }

    /**
     * @param string|null $sql
     * @param string $key
     * @return array
     */
    public function queryAllAssocKey($sql, $key) {
        $rows = array();
        $stmt = $this->query($sql);
        if ($stmt) {
            while (($row = $stmt->fetch(PDO::FETCH_ASSOC)) !== false)
                $rows[$row[$key]] = $row;
        }
        $this->release();
        $this->memoryUsageAfterFetch = memory_get_usage();
        //\MNLogger\TraceLogger::instance('trace')->MYSQL_CR($this->lastStmt ? 'SUCCESS' : 'EXCEPTION', $this->memoryUsageAfterFetch - $this->memoryUsageBeforeFetch);
        return $rows;
    }

    /**
     * @param string|null $sql
     * @param string $key
     * @return array
     */
    public function queryAll($sql = null, $key = '') {
        if ($key)
            return $this->queryAllAssocKey($sql, $key);

        $stmt = $this->query($sql);
        $data = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->release();
        $this->memoryUsageAfterFetch = memory_get_usage();
        //\MNLogger\TraceLogger::instance('trace')->MYSQL_CR($this->lastStmt ? 'SUCCESS' : 'EXCEPTION', $this->memoryUsageAfterFetch - $this->memoryUsageBeforeFetch);
        return $data;
    }

    public function find($table, $cond, $order = '') {
        if (is_numeric($cond))
            $cond = array('id' => "$cond");
        $table = $this->quoteObj($table);
        $where = $this->buildWhere($cond);

        if ($order && strncasecmp($order, 'ORDER BY', 8) != 0)
            $order = 'ORDER BY ' . $order;
        $sql = "SELECT * FROM $table $where $order";
        return $this->queryRow($sql);
    }

    public function findAll($table, $cond, $order = '') {
        $table = $this->quoteObj($table);
        $where = $this->buildWhere($cond);
        if ($order && strncasecmp($order, 'ORDER BY', 8) != 0)
            $order = 'ORDER BY ' . $order;
        $sql = "SELECT * FROM $table $where $order";
        return $this->queryAll($sql);
    }

    public function count($table, $cond, $columns = '*') {
        $table = $this->quoteObj($table);
        $where = $this->buildWhere($cond);
        $sql = "SELECT COUNT($columns) FROM $table $where";
        return $this->querySimple($sql);
    }

    //general implemention
    public function exists($table, $cond) {
        $table = $this->quoteObj($table);
        $where = $this->buildWhere($cond);
        $sql = "SELECT 1 FROM $table $where LIMIT 1";
        return !!$this->querySimple($sql);
    }

    /**
     * @param $a
     * @param null $b
     * @return \Db\Connection
     */
    public function limit($a, $b = null) {
        if (is_null($b)) {
            $a = intval($a);
            $this->select_sql_order_limit .= " LIMIT $a ";
        } else {
            $a = intval($a);
            $b = intval($b);
            $this->select_sql_order_limit .= " LIMIT $a, $b ";
        }
        return $this;
    }

    public function logQuery($sql) {
        if (isset(static::$configs['DEBUG']) && static::$configs['DEBUG'] && isset(static::$configs['DEBUG_LEVEL'])) {
            $logString = 'Begin:' . date('Y-m-d H:i:s', $this->queryBeginTime) . "\n";
            $logString .= 'SQL: ' . $sql . "\n";
            switch (static::$configs['DEBUG_LEVEL']) {
                case 2 :
                    //looks ugly
                    $tempE = new \Exception ();
                    $logString .= "Trace:\n" . $tempE->getTraceAsString() . "\n";
                    continue;
                case 1 :
                default :
                    continue;
            }
            $logString .= 'End:' . date('Y-m-d H:i:s', $this->queryEndTime) . '  Total:' . sprintf('%.3f', ($this->queryEndTime - $this->queryBeginTime) * 1000) . 'ms' . "\n";
            //\Log\Handler::instance('db')->log($logString);
        }
    }

    public static function getExternalCaller() {
        $trace = debug_backtrace(false);
        $caller = array('class' => '', 'method' => '', 'file' => '', 'line' => '');
        $k = 0;
        foreach ($trace as $k => $line) {
            if (isset($line['class']) && strpos($line['class'], __NAMESPACE__) === 0) {
                continue;
            } else if (isset($line['class'])) {
                $caller['class'] = $line['class'];
                $caller['method'] = $line['function'];
            } else if (isset($line['function'])) {
                $caller['method'] = $line['function'];
            } else {
                $caller['class'] = 'main';
            }
            break;
        }
        if (empty($caller['method'])) {
            $caller['method'] = 'main';
        }
        while (!isset($line['file']) && $k > 0) {// 可能在eval或者call_user_func里调用的。
            $line = $trace[--$k];
        }
        $caller['file'] = $line['file'];
        $caller['line'] = $line['line'];
        return $caller;
    }

    /**
     *
     * @param int $pageNumber
     * @param int $rowsPerPage
     * @param string $countColumnsOrSqlCount
     * @param string $sqlForQueryWithoutLimit
     * @return JMDbConnectionPageResult
     */
    public function getPageResultByNumber($pageNumber, $rowsPerPage, $countColumnsOrSqlCount = '*', $sqlForQueryWithoutLimit = null, $primaryKey = '', $sort = 'ASC') {
        if ($pageNumber <= 0)
            $pageNumber = 1;
        return $this->getPageResultByIndex($pageNumber - 1, $rowsPerPage, $countColumnsOrSqlCount, $sqlForQueryWithoutLimit, $primaryKey, $sort);
    }

    /**
     * 说明：对于有GROUP BY id的查询，需要用 COUNT(DISTINCT id)获取结果集总数，也就是说需要传递第三个参数
     * @param int $pageIndex
     * @param int $rowsPerPage
     * @param string $countColumnsOrSqlForCount
     * @param string $sqlForQueryWithoutLimit
     * @return JMDbConnectionPageResult
     */
    public function getPageResultByIndex($pageIndex, $rowsPerPage, $countColumnsOrSqlForCount = '*', $sqlForQueryWithoutLimit = null, $primaryKey = '', $sort = 'ASC') {
        if ($rowsPerPage < 1)
            $rowsPerPage = 1;
        $o = new JMDbConnectionPageResult();
        if ($pageIndex <= 0)
            $pageIndex = 0;

        if ($sqlForQueryWithoutLimit) {
            $sqlForCount = $countColumnsOrSqlForCount;
            $o->rowCount = intval($this->querySimple($sqlForCount));
            $sqlForQuery = $sqlForQueryWithoutLimit . " LIMIT " . ($pageIndex * $rowsPerPage) . ", " . intval($rowsPerPage);
        } else { // no $sqlForCount, use the chain sql mode
            $sqlForCount = "SELECT COUNT($countColumnsOrSqlForCount) {$this->select_sql_from_where}"; // 说明：对于有GROUP BY id的查询，需要用 COUNT(DISTINCT id)获取结果集总数
            $o->rowCount = intval($this->querySimple($sqlForCount));
            if (empty($primaryKey)) {
                $sqlForQuery = "SELECT {$this->select_sql_columns} {$this->select_sql_from_where} {$this->select_sql_group_having} {$this->select_sql_order_limit} LIMIT " . ($pageIndex * $rowsPerPage) . ", " . intval($rowsPerPage);
            } else {

                $select_sql_from_where = $this->select_sql_from_where;
                if (!stristr($this->select_sql_from_where, 'where')) {
                    $select_sql_from_where .= ' WHERE 1=1';
                }
                $op = " >= ";
                if (strtolower($sort) == 'desc') {
                    $op = " <= ";
                }

                $select_sql_order_limit = $this->select_sql_order_limit;

                $limitRowsNumber = $pageIndex * $rowsPerPage;
                if ($limitRowsNumber >= $o->rowCount)
                    $limitRowsNumber = $o->rowCount - 1;
                if ($limitRowsNumber < 0)
                    $limitRowsNumber = 0;
                if (($o->rowCount / 2) < $limitRowsNumber && stristr($select_sql_order_limit, 'order')) {

                    if (stristr($select_sql_order_limit, 'desc')) {
                        $select_sql_order_limit = str_ireplace('desc', 'ASC', $select_sql_order_limit);
                    } else if (stristr($select_sql_order_limit, 'asc')) {
                        $select_sql_order_limit = str_ireplace('asc', 'DESC', $select_sql_order_limit);
                    }

                    $select_sql_order_limit .= "LIMIT " . ($o->rowCount - $limitRowsNumber - 1) . ", 1";
                } else {
                    $select_sql_order_limit .= "LIMIT " . ($limitRowsNumber) . ", 1";
                }

                $select_sql_from_where .= " AND " . $primaryKey . "{$op} (SELECT {$primaryKey} {$this->select_sql_from_where} {$this->select_sql_group_having} {$select_sql_order_limit})";
                $sqlForQuery = "SELECT {$this->select_sql_columns} {$select_sql_from_where} {$this->select_sql_group_having} {$this->select_sql_order_limit} LIMIT " . intval($rowsPerPage);
            }
        }

        $o->pageCount = ceil($o->rowCount / $rowsPerPage);
        $o->rows = $this->queryAll($sqlForQuery);
        $o->pageIndex = $pageIndex;
        $o->pageNumber = $pageIndex + 1;
        $o->rowsPerPage = $rowsPerPage;
        return $o;
    }

    public function release($force = false) {
        if ($this->usePool && $this->connection && ($this::$inTrans === FALSE || $force)) {
            $this->connection->release();
        }
    }

    protected function getOneSlave(array $arr) {
        $tmp = array();
        foreach ($arr as $value) {
            if ((int) $value['weight'] > 1) {
                for ($i = 0; $i < (int) $value['weight']; $i++) {
                    $tmp[] = $value;
                }
            } else {
                $tmp[] = $value;
            }
        }
        shuffle($tmp);
        return $tmp[0];
    }

}
