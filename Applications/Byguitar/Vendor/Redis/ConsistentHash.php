<?php
namespace Redis;

use \Exception;
/*
 * 改的一个老外写的算法,后续有时间把他集成到redis扩展里面提高效率
 */

/**
 * 一致性hash算法
 *
 * @author xinhuag@jumei.com
 */
class ConsistentHash {

    /**
     * The number of positions to hash each target to.
     *
     * @var int
     * @comment 虚拟节点数,解决节点分布不均的问题 缓解雪崩效应
     */
    private $_replicas = 100;

    /**
     * Internal counter for current number of targets.
     * @var int
     * @comment 节点记数器
     */
    private $_targetCount = 0;

    /*
     * 缓存下，如果没变的话就不生成圆环排序
     */
    private $_target = array();

    /**
     * Internal map of positions (hash outputs) to targets
     * @var array { position => target, ... }
     * @comment 位置对应节点,用于lookup中根据位置确定要访问的节点
     */
    private $_positionToTarget = array();

    /**
     * Internal map of targets to lists of positions that target is hashed to.
     * @var array { target => [ position, position, ... ], ... }
     * @comment 节点对应位置,用于删除节点
     */
    private $_targetToPositions = array();

    /**
     * Whether the internal map of positions to targets is already sorted.
     * @var boolean
     * @comment 是否已排序
     */
    private $_positionToTargetSorted = false;

    /**
     * @comment 构造函数
     */
    public function __construct() {

    }

    /**
     * Add a target.
     * @param string $target
     * @chainable
     * @comment 添加节点,根据虚拟节点数,将节点分布到多个虚拟位置上
     */
    public function addTarget($target) {
        if (isset($this->_targetToPositions[$target])) {
            throw new Exception("Target '$target' already exists.");
        }

        $this->_targetToPositions[$target] = array();

        // hash the target into multiple positions
        for ($i = 0; $i < $this->_replicas; $i++) {
            $position = $this->hash($target . $i);
            $this->_positionToTarget[$position] = $target; // lookup
            $this->_targetToPositions[$target] [] = $position; // target removal
        }

        $this->_positionToTargetSorted = false;
        $this->_targetCount++;

        return $this;
    }

    /**
     * 添加结点信息
     * @param array $targets
     * @chainable
     */
    public function addTargets($targets) {
        $this->_target = $targets;
        $count = count($targets);
        $this->_replicas = floor(1000/$count);//保证结点不是特别多影响效率

        foreach ($targets as $target) {
            $this->addTarget($target);
        }

        return $this;
    }

    /**
     * 移除结点
     * @param string $target
     * @chainable
     */
    public function removeTarget($target) {
        if (!isset($this->_targetToPositions[$target])) {
            throw new Exception("Target '$target' does not exist.");
        }

        foreach ($this->_targetToPositions[$target] as $position) {
            unset($this->_positionToTarget[$position]);
        }

        unset($this->_targetToPositions[$target]);

        $this->_targetCount--;

        return $this;
    }

    /**
     * 返回所有的实际结点
     * @return array
     */
    public function getAllTargets() {
        return array_keys($this->_targetToPositions);
    }

    /**
     * 返回所有的实际结点
     * @return array
     */
    public function getVTargets() {
        return $this->_targetToPositions;
    }

    /**
     * 查找结点
     * @param string $resource
     * @return string
     */
    public function lookup($resource) {
        $targets = $this->lookupList($resource, 1);
        if (empty($targets)) {
            throw new Exception('No targets exist');
        }
        return $targets[0];
    }

    /**
     * Get a list of targets for the resource, in order of precedence.
     * Up to $requestedCount targets are returned, less if there are fewer in total.
     *
     * @param string $resource
     * @param int $requestedCount The length of the list to return
     * @return array List of targets
     * @comment 查找当前的资源对应的节点,
     *          节点为空则返回空,节点只有一个则返回该节点,
     *          对当前资源进行hash,对所有的位置进行排序,在有序的位置列上寻找当前资源的位置
     *          当全部没有找到的时候,将资源的位置确定为有序位置的第一个(形成一个环)
     *          返回所找到的节点
     */
    public function lookupList($resource, $requestedCount) {
        if (!$requestedCount) {
            throw new Exception('Invalid count requested');
        }

        // handle no targets
        if (empty($this->_positionToTarget)) {
            return array();
        }

        // optimize single target
        if ($this->_targetCount == 1) {
            return array_unique(array_values($this->_positionToTarget));
        }

        // hash resource to a position
        $resourcePosition = $this->hash($resource);

        $results = array();
        $collect = false;

        $this->_sortPositionTargets();

        
        foreach ($this->_positionToTarget as $key => $value) {
            // start collecting targets after passing resource position
            if ($key > $resourcePosition) {
                $result[] = $value;
                return $result;
            }
            }
        // loop to start - search values below the resourcePosition
        foreach ($this->_positionToTarget as $key => $value) {
             $result[] = $value;
             return $result;
            }
//        // search values above the resourcePosition
//        foreach ($this->_positionToTarget as $key => $value) {
//            // start collecting targets after passing resource position
//            if (!$collect && $key > $resourcePosition) {
//                $collect = true;
//            }
//
//            // only collect the first instance of any target
//            if ($collect && !in_array($value, $results)) {
//                $results [] = $value;
//            }
//
//            // return when enough results, or list exhausted
//            if (count($results) == $requestedCount || count($results) == $this->_targetCount) {
//                return $results;
//            }
//        }
//
//        // loop to start - search values below the resourcePosition
//        foreach ($this->_positionToTarget as $key => $value) {
//            if (!in_array($value, $results)) {
//                $results [] = $value;
//            }
//
//            // return when enough results, or list exhausted
//            if (count($results) == $requestedCount || count($results) == $this->_targetCount) {
//                return $results;
//            }
//        }
//
//        // return results after iterating through both "parts"
//        return $results;
    }

    public function __toString() {
        return sprintf(
                '%s{targets:[%s]}', get_class($this), implode(',', $this->getAllTargets())
        );
    }

    /**
     * 按照key进行排序
     */
    private function _sortPositionTargets() {
        if (!$this->_positionToTargetSorted) {
            ksort($this->_positionToTarget, SORT_REGULAR);
            $this->_positionToTargetSorted = true;
        }
    }

    public function hash($string) {//比md5方式快
        return md5($string); // 8 hexits = 32bit
    }

}
