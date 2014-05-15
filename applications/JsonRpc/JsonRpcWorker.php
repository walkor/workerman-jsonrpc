<?php
define('ROOT_DIR', __DIR__);
require_once ROOT_DIR . '/Protocols/JsonProtocol.php';
require_once ROOT_DIR . '/Clients/StatisticClient.php';

/**
 * 
 *  JsonRpcWorker，Rpc服务的入口文件
 *  根据客户端传递参数调用 applications/JsonRpc/Services/目录下的文件的类的方法
 *  
 * @author walkor <worker-man@qq.com>
 */
class JsonRpcWorker extends Man\Core\SocketWorker
{
    
    /**
     * 统计数据上报的地址
     * @var string
     */
    protected $statisticAddress = 'udp://127.0.0.1:33636';
    
    /**
     * 启动的时候初始化统计上报地址)
     * @see Man\Core.SocketWorker::onStart()
     */
    public function onStart()
    {
        // 获取统计上报地址
        $statistic_address = \Man\Core\Lib\Config::get($this->workerName.'.statistic_address');
        if($statistic_address)
        {
            $this->statisticAddress = $statistic_address;
        }
    }
    
    /**
     * 确定数据是否接收完整
     * @see Man\Core.SocketWorker::dealInput()
     */
    public function dealInput($recv_str)
    {
        return JsonProtocol::dealInput($recv_str); 
    }

    /**
     * 数据接收完整后处理业务逻辑
     * @see Man\Core.SocketWorker::dealProcess()
     */
    public function dealProcess($recv_str)
    {
        /**
         * data的数据格式为
         * ['class'=>xx, 'method'=>xx, 'param_array'=>array(xx)]
         * @var array
         */
        $data = JsonProtocol::decode($recv_str);
        // 判断数据是否正确
        if(empty($data['class']) || empty($data['method']) || !isset($data['param_array']))
        {
            // 发送数据给客户端，请求包错误
            return $this->sendToClient(JsonProtocol::encode(array('code'=>400, 'msg'=>'bad request', 'data'=>null)));
        }
        // 获得要调用的类、方法、及参数
        $class = $data['class'];
        $method = $data['method'];
        $param_array = $data['param_array'];
        
        StatisticClient::tick($class, $method);
        $success = false;
        // 判断类对应文件是否载入
        if(!class_exists($class))
        {
            $include_file = ROOT_DIR . "/Services/$class.php";
            if(is_file($include_file))
            {
                require_once $include_file;
            }
            if(!class_exists($class))
            {
                $code = 404;
                $msg = "class $class not found";
                StatisticClient::report($class, $method, $success, $code, $msg, $this->statisticAddress);
                // 发送数据给客户端 类不存在
                return $this->sendToClient(JsonProtocol::encode(array('code'=>$code, 'msg'=>$msg, 'data'=>null)));
            }
        }
        
        // 调用类的方法
        try 
        {
            $ret = call_user_func_array(array($class, $method), $param_array);
            StatisticClient::report($class, $method, 1, 0, '', $this->statisticAddress);
            // 发送数据给客户端，调用成功，data下标对应的元素即为调用结果
            return $this->sendToClient(JsonProtocol::encode(array('code'=>0, 'msg'=>'ok', 'data'=>$ret)));
        }
        // 有异常
        catch(Exception $e)
        {
            // 发送数据给客户端，发生异常，调用失败
            $code = $e->getCode() ? $e->getCode() : 500;
            StatisticClient::report($class, $method, $success, $code, $e, $this->statisticAddress);
            return $this->sendToClient(JsonProtocol::encode(array('code'=>$code, 'msg'=>$e->getMessage(), 'data'=>$e)));
        }
    }
}
