<?php
/**
 * This file is part of workerman.
 *
 * Licensed under The MIT License
 * For full copyright and license information, please see the LICENSE
 * Redistributions of files must retain the above copyright notice.
 *
 * @author    walkor<walkor@workerman.net>
 * @copyright walkor<walkor@workerman.net>
 * @link      http://www.workerman.net/
 * @license   http://www.opensource.org/licenses/mit-license.php MIT License
 */
namespace Workerman\Redis;

use Amp\DeferredFuture;
use Workerman\Connection\AsyncTcpConnection;
use Workerman\Timer;

/**
 * Class Client
 * @package Workerman\Redis
 *
 * @method bool select($db)
 * @method bool auth($password)
 *
 * Strings methods
 * @method int append($key, $value)
 * @method int bitCount($key)
 * @method int decr($key)
 * @method int decrBy($key, $value)
 * @method string|bool get($key)
 * @method int getBit($key, $offset)
 * @method string getRange($key, $start, $end)
 * @method string getSet($key, $value)
 * @method int incr($key)
 * @method int incrBy($key, $value)
 * @method float incrByFloat($key, $value)
 * @method array mGet(array $keys)
 * @method array getMultiple(array $keys)
 * @method bool setBit($key, $offset, $value)
 * @method bool setEx($key, $ttl, $value)
 * @method bool pSetEx($key, $ttl, $value)
 * @method bool setNx($key, $value)
 * @method string setRange($key, $offset, $value)
 * @method int strLen($key)
 * Keys methods
 * @method int del(...$keys)
 * @method int unlink(...$keys)
 * @method false|string dump($key)
 * @method int exists(...$keys)
 * @method bool expire($key, $ttl)
 * @method bool pexpire($key, $ttl)
 * @method bool expireAt($key, $timestamp)
 * @method bool pexpireAt($key, $timestamp)
 * @method array keys($pattern)
 * @method bool|array scan($it)
 * @method void migrate($host, $port, $keys, $dbIndex, $timeout, $copy = false, $replace = false)
 * @method bool move($key, $dbIndex)
 * @method string|int|bool object($information, $key)
 * @method bool persist($key)
 * @method string randomKey()
 * @method bool rename($srcKey, $dstKey)
 * @method bool renameNx($srcKey, $dstKey)
 * @method string type($key)
 * @method int ttl($key)
 * @method int pttl($key)
 * @method void restore($key, $ttl, $value)
 * Hashes methods
 * @method false|int hSet($key, $hashKey, $value)
 * @method bool hSetNx($key, $hashKey, $value)
 * @method false|string hGet($key, $hashKey)
 * @method false|int hLen($key)
 * @method false|int hDel($key, ...$hashKeys)
 * @method array hKeys($key)
 * @method array hVals($key)
 * @method bool hExists($key, $hashKey)
 * @method int hIncrBy($key, $hashKey, $value)
 * @method float hIncrByFloat($key, $hashKey, $value)
 * @method array hScan($key, $iterator, $pattern = '', $count = 0)
 * @method int hStrLen($key, $hashKey)
 * Lists methods
 * @method array blPop($keys, $timeout)
 * @method array brPop($keys, $timeout)
 * @method false|string bRPopLPush($srcKey, $dstKey, $timeout)
 * @method false|string lIndex($key, $index)
 * @method int lInsert($key, $position, $pivot, $value)
 * @method false|string lPop($key)
 * @method false|int lPush($key, ...$entries)
 * @method false|int lPushx($key, $value)
 * @method array lRange($key, $start, $end)
 * @method false|int lRem($key, $value, $count)
 * @method bool lSet($key, $index, $value)
 * @method false|array lTrim($key, $start, $end)
 * @method false|string rPop($key)
 * @method false|string rPopLPush($srcKey, $dstKey)
 * @method false|int rPush($key, ...$entries)
 * @method false|int rPushX($key, $value)
 * @method false|int lLen($key)
 * Sets methods
 * @method int sAdd($key, $value)
 * @method int sCard($key)
 * @method array sDiff($keys)
 * @method false|int sDiffStore($dst, $keys)
 * @method false|array sInter($keys)
 * @method false|int sInterStore($dst, $keys)
 * @method bool sIsMember($key, $member)
 * @method array sMembers($key)
 * @method bool sMove($src, $dst, $member)
 * @method false|string|array sPop($key, $count = 0)
 * @method false|string|array sRandMember($key, $count = 0)
 * @method int sRem($key, ...$members)
 * @method array sUnion(...$keys)
 * @method false|int sUnionStore($dst, ...$keys)
 * @method false|array sScan($key, $iterator, $pattern = '', $count = 0)
 * Sorted sets methods
 * @method array bzPopMin($keys, $timeout)
 * @method array bzPopMax($keys, $timeout)
 * @method int zAdd($key, $score, $value)
 * @method int zCard($key)
 * @method int zCount($key, $start, $end)
 * @method double zIncrBy($key, $value, $member)
 * @method int zinterstore($keyOutput, $arrayZSetKeys, $arrayWeights = [], $aggregateFunction = '')
 * @method array zPopMin($key, $count)
 * @method array zPopMax($key, $count)
 * @method array zRange($key, $start, $end, $withScores = false)
 * @method array zRangeByScore($key, $start, $end, $options = [])
 * @method array zRevRangeByScore($key, $start, $end, $options = [])
 * @method array zRangeByLex($key, $min, $max, $offset = 0, $limit = 0)
 * @method int zRank($key, $member)
 * @method int zRevRank($key, $member)
 * @method int zRem($key, ...$members)
 * @method int zRemRangeByRank($key, $start, $end)
 * @method int zRemRangeByScore($key, $start, $end)
 * @method array zRevRange($key, $start, $end, $withScores = false)
 * @method double zScore($key, $member)
 * @method int zunionstore($keyOutput, $arrayZSetKeys, $arrayWeights = [], $aggregateFunction = '')
 * @method false|array zScan($key, $iterator, $pattern = '', $count = 0)
 * HyperLogLogs methods
 * @method int pfAdd($key, $values)
 * @method int pfCount($keys)
 * @method bool pfMerge($dstKey, $srcKeys)
 * Geocoding methods
 * @method int geoAdd($key, $longitude, $latitude, $member, ...$items)
 * @method array geoHash($key, ...$members)
 * @method array geoPos($key, ...$members)
 * @method double geoDist($key, $members, $unit = '')
 * @method int|array geoRadius($key, $longitude, $latitude, $radius, $unit, $options = [])
 * @method array geoRadiusByMember($key, $member, $radius, $units, $options = [])
 * Streams methods
 * @method int xAck($stream, $group, $arrMessages)
 * @method string xAdd($strKey, $strId, $arrMessage, $iMaxLen = 0, $booApproximate = false)
 * @method array xClaim($strKey, $strGroup, $strConsumer, $minIdleTime, $arrIds, $arrOptions = [])
 * @method int xDel($strKey, $arrIds)
 * @method mixed xGroup($command, $strKey, $strGroup, $strMsgId, $booMKStream = null)
 * @method mixed xInfo($command, $strStream, $strGroup = null)
 * @method int xLen($stream)
 * @method array xPending($strStream, $strGroup, $strStart = 0, $strEnd = 0, $iCount = 0, $strConsumer = null)
 * @method array xRange($strStream, $strStart, $strEnd, $iCount = 0)
 * @method array xRead($arrStreams, $iCount = 0, $iBlock = null)
 * @method array xReadGroup($strGroup, $strConsumer, $arrStreams, $iCount = 0, $iBlock = null)
 * @method array xRevRange($strStream, $strEnd, $strStart, $iCount = 0)
 * @method int xTrim($strStream, $iMaxLen, $booApproximate = null)
 * Pub/sub methods
 * @method mixed publish($channel, $message)
 * @method mixed pubSub($keyword, $argument = null)
 * Generic methods
 * @method mixed rawCommand(...$commandAndArgs)
 * Transactions methods
 * @method \Redis multi()
 * @method mixed exec()
 * @method mixed discard()
 * @method mixed watch($keys)
 * @method mixed unwatch($keys)
 * Scripting methods
 * @method mixed eval($script, $args = [], $numKeys = 0)
 * @method mixed evalSha($sha, $args = [], $numKeys = 0)
 * @method mixed script($command, ...$scripts)
 * @method mixed client(...$args)
 * @method null|string getLastError()
 * @method bool clearLastError()
 * @method mixed _prefix($value)
 * @method mixed _serialize($value)
 * @method mixed _unserialize($value)
 * Introspection methods
 * @method bool isConnected()
 * @method mixed getHost()
 * @method mixed getPort()
 * @method false|int getDbNum()
 * @method false|double getTimeout()
 * @method mixed getReadTimeout()
 * @method mixed getPersistentID()
 * @method mixed getAuth()
 */
class Client
{
    /**
     * @var AsyncTcpConnection
     */
    protected $_connection = null;

    /**
     * @var array
     */
    protected $_options = [];

    /**
     * @var string
     */
    protected $_address = '';

    /**
     * @var array
     */
    protected $_queue = [];

    /**
     * @var int
     */
    protected $_db = 0;

    /**
     * @var string|array
     */
    protected $_auth = null;

    /**
     * @var bool
     */
    protected $_waiting = true;

    /**
     * @var Timer
     */
    protected $_connectTimeoutTimer = null;

    /**
     * @var Timer
     */
    protected $_reconnectTimer = null;

    /**
     * @var callable
     */
    protected $_connectionCallback = null;

    /**
     * @var Timer
     */
    protected $_waitTimeoutTimer = null;

    /**
     * @var string
     */
    protected $_error = '';

    /**
     * @var bool
     */
    protected $_subscribe = false;

    /**
     * @var bool
     */
    protected $_firstConnect = true;

    /**
     * Client constructor.
     * @param $address
     * @param array $options
     * @param null $callback
     */
    public function __construct($address, $options = [], $callback = null)
    {
        if (!\class_exists('Protocols\Redis')) {
            \class_alias('Workerman\Redis\Protocols\Redis', 'Protocols\Redis');
        }
        $this->_address = $address;
        $this->_options = $options;
        $this->_connectionCallback = $callback;
        $this->connect();
        $timer = Timer::add(1, function () use (&$timer) {
            if (empty($this->_queue)) {
                return;
            }
            if ($this->_subscribe) {
                Timer::del($timer);
                return;
            }
            reset($this->_queue);
            $current_queue = current($this->_queue);
            $current_command = $current_queue[0][0];
            $ignore_first_queue = in_array($current_command, ['BLPOP', 'BRPOP']);
            $time = time();
            $timeout = isset($this->_options['wait_timeout']) ? $this->_options['wait_timeout'] : 600;
            $has_timeout = false;
            $first_queue = true;
            foreach ($this->_queue as $key => $queue) {
                if ($first_queue && $ignore_first_queue) {
                    $first_queue = false;
                    continue;
                }
                if ($time - $queue[1] > $timeout) {
                    $has_timeout = true;
                    unset($this->_queue[$key]);
                    $msg = "Workerman Redis Wait Timeout ($timeout seconds)";
                    if ($queue[2]) {
                        $this->_error = $msg;
                        \call_user_func($queue[2], false, $this);
                    } else {
                        echo new Exception($msg);
                    }
                }
            }
            if ($has_timeout && !$ignore_first_queue) {
                $this->closeConnection();
                $this->connect();
            }
        });
    }

    /**
     * connect
     */
    public function connect()
    {
        if ($this->_connection) {
            return;
        }

        $timeout = isset($this->_options['connect_timeout']) ? $this->_options['connect_timeout'] : 5;
        $context = isset($this->_options['context']) ? $this->_options['context'] : [];
        $this->_connection = new AsyncTcpConnection($this->_address, $context);

        $this->_connection->onConnect = function () {
            $this->_waiting = false;
            Timer::del($this->_connectTimeoutTimer);
            if ($this->_reconnectTimer) {
                Timer::del($this->_reconnectTimer);
                $this->_reconnectTimer = null;
            }

            if ($this->_db) {
                $this->_queue = \array_merge([[['SELECT', $this->_db], time(), null]], $this->_queue);
            }

            if ($this->_auth) {
                $this->_queue = \array_merge([[['AUTH', $this->_auth], time(), null]],  $this->_queue);
            }

            $this->_connection->onError = function ($connection, $code, $msg) {
                echo new \Exception("Workerman Redis Connection Error $code $msg");
            };
            $this->process();
            $this->_firstConnect && $this->_connectionCallback && \call_user_func($this->_connectionCallback, true, $this);
            $this->_firstConnect = false;
        };

        $time_start = microtime(true);
        $this->_connection->onError = function ($connection) use ($time_start) {
            $time = microtime(true) - $time_start;
            $msg = "Workerman Redis Connection Failed ($time seconds)";
            $this->_error = $msg;
            $exception = new \Exception($msg);
            if (!$this->_connectionCallback) {
                echo $exception;
                return;
            }
            $this->_firstConnect && \call_user_func($this->_connectionCallback, false, $this);
        };

        $this->_connection->onClose = function () use ($time_start) {
            $this->_subscribe = false;
            if ($this->_connectTimeoutTimer) {
                Timer::del($this->_connectTimeoutTimer);
            }
            if ($this->_reconnectTimer) {
                Timer::del($this->_reconnectTimer);
                $this->_reconnectTimer = null;
            }
            $this->closeConnection();
            if (microtime(true) - $time_start > 5) {
                $this->connect();
            } else {
                $this->_reconnectTimer = Timer::add(5, function () {
                    $this->connect();
                }, null, false);
            }
        };

        $this->_connection->onMessage = function ($connection, $data) {
            $this->_error = '';
            $this->_waiting = false;
            reset($this->_queue);
            $queue = current($this->_queue);
            $cb = $queue[2];
            $type = $data[0];
            if (!$this->_subscribe) {
                unset($this->_queue[key($this->_queue)]);
            }
            if (empty($this->_queue)) {
                $this->_queue = [];
                gc_collect_cycles();
                if (function_exists('gc_mem_caches')) {
                    gc_mem_caches();
                }
            }
            $success = $type === '-' || $type === '!' ? false : true;
            $exception = false;
            $result = false;
            if ($success) {
                $result = $data[1];
                if ($type === '+' && $result === 'OK') {
                    $result = true;
                }
            } else {
                $this->_error = $data[1];
            }
            if (!$cb) {
                $this->process();
                return;
            }
            // format.
            if (!empty($queue[3])) {
                $result = \call_user_func($queue[3], $result);
            }
            try {
                \call_user_func($cb, $result, $this);
            } catch (\Exception $exception) {
            }

            if ($type === '!') {
                $this->closeConnection();
                $this->connect();
            } else {
                $this->process();
            }
            if ($exception) {
                throw $exception;
            }
        };

        $this->_connectTimeoutTimer = Timer::add($timeout, function () use ($timeout) {
            $this->_connectTimeoutTimer = null;
            if ($this->_connection && $this->_connection->getStatus(false) === 'ESTABLISHED') {
                return;
            }
            $this->closeConnection();
            $this->_error = "Workerman Redis Connection to {$this->_address} timeout ({$timeout} seconds)";
            if ($this->_firstConnect && $this->_connectionCallback) {
                \call_user_func($this->_connectionCallback, false, $this);
            } else {
                echo $this->_error . "\n";
            }

        });
        $this->_connection->connect();
    }

    /**
     * process
     */
    public function process()
    {
        if (!$this->_connection || $this->_waiting || empty($this->_queue) || $this->_subscribe) {
            return;
        }
        \reset($this->_queue);
        $queue = \current($this->_queue);
        if ($queue[0][0] === 'SUBSCRIBE' || $queue[0][0] === 'PSUBSCRIBE') {
            $this->_subscribe = true;
        }
        $this->_waiting = true;
        $this->_connection->send($queue[0]);
        $this->_error = '';
    }

    /**
     * subscribe
     *
     * @param $channels
     * @param $cb
     */
    public function subscribe($channels, $cb)
    {
        $new_cb = function ($result) use ($cb) {
            if (!$result) {
                echo $this->error();
                return;
            }
            $response_type = $result[0];
            switch ($response_type) {
                case 'subscribe':
                    return;
                case 'message':
                    \call_user_func($cb, $result[1], $result[2], $this);
                    return;
                default:
                    echo 'unknow response type for subscribe. buffer:' . serialize($result) . "\n";
            }
        };
        $this->_queue[] = [['SUBSCRIBE', $channels], time(), $new_cb];
        $this->process();
    }

    /**
     * psubscribe
     *
     * @param $patterns
     * @param $cb
     */
    public function pSubscribe($patterns, $cb)
    {
        $new_cb = function ($result) use ($cb) {
            if (!$result) {
                echo $this->error();
                return;
            }
            $response_type = $result[0];
            switch ($response_type) {
                case 'psubscribe':
                    return;
                case 'pmessage':
                    \call_user_func($cb, $result[1], $result[2], $result[3], $this);
                    return;
                default:
                    echo 'unknow response type for psubscribe. buffer:' . serialize($result) . "\n";
            }
        };
        $this->_queue[] = [['PSUBSCRIBE', $patterns], time(), $new_cb];
        $this->process();
    }

    /**
     * set
     *
     * @param string $key
     * @param string $value
     * @param int $ex
     * @param int $px
     * @param string $opt NX or XX
     * @return bool
     */
    public function set($key, $value, $ex=null, $px=null, $opt=null)
    {
        $args = [$key, $value];
        $ex !== null && array_push($args, 'EX', $ex);
        $px !== null && array_push($args, 'PX', $px);
        $opt && $args[] = $opt;
        return $this->__call('SET', $args);
    }

    /**
     * sort
     *
     * @param $key
     * @param $options
     * @param null $cb
     */
    function sort($key, $options)
    {
        $args = [$key];
        if (isset($options['sort'])) {
            $args[] = $options['sort'];
            unset($options['sort']);
        }

        foreach ($options as $op => $value) {
            $args[] = $op;
            if (!is_array($value)) {
                $args[] = $value;
                continue;
            }
            foreach ($value as $sub_value) {
                $args[] = $sub_value;
            }
        }

        return $this->__call('SORT', $args);
    }

    /**
     * mSet
     *
     * @param array $array
     * @return bool
     */
    public function mSet(array $array)
    {
        return $this->mapCb('MSET', $array);
    }

    /**
     * mSetNx
     *
     * @param array $array
     * @return int
     */
    public function mSetNx(array $array)
    {
        return $this->mapCb('MSETNX', $array);
    }

    /**
     * mapCb
     *
     * @param string $command
     * @param array $array
     * @return mixed
     */
    protected function mapCb($command, array $array)
    {
        $args = [$command];
        foreach ($array as $key => $value) {
            $args[] = $key;
            $args[] = $value;
        }
        return $this->__call($command, $args);
    }

    /**
     * hMSet
     *
     * @param string $key
     * @param array $array
     * @return bool
     */
    public function hMSet($key, array $array)
    {
        $args = [$key];

        foreach ($array as $k => $v) {
            array_push($args, $k, $v);
        }

        return $this->__call('HMSET', $args);
    }

    /**
     * hMGet
     *
     * @param $key
     * @param array $fields
     * @return array
     */
    public function hMGet($key, array $fields)
    {
        $result = $this->__call('HMGET', array_merge($key, $fields));
        if (!is_array($result)) {
            return $result;
        }
        return array_combine($fields, $result);
    }

    /**
     * hGetAll
     *
     * @param $key
     * @param null $cb
     */
    public function hGetAll($key)
    {
        $result = $this->__call('HGETALL', [$key]);

        if (!is_array($result)) {
            return $result;
        }

        $data = [];

        foreach (array_chunk($result, 2) as $row) {
            list($k, $v) = $row;
            $data[$k] = $v;
        }

        return $data;
    }

    /**
     * __call
     *
     * @param $method
     * @param $args
     * @return mixed
     */
    public function __call($method, $args)
    {
        \array_unshift($args, \strtoupper($method));

        $defer = new DeferredFuture;

        $cb = function($result) use ($defer) {
            if ($result !== false) {
                $defer->complete($result);
            } else {
                $defer->error(new Exception($this->error()));
            }
        };

        $this->_queue[] = [$args, time(), $cb];
        $this->process();

        return $defer->getFuture()->await();
    }

    /**
     * closeConnection
     */
    public function closeConnection()
    {
        if (!$this->_connection) {
            return;
        }
        $this->_subscribe = false;
        $this->_connection->onConnect = $this->_connection->onError = $this->_connection->onClose =
        $this->_connection->onMessge = null;
        $this->_connection->close();
        $this->_connection = null;
        if ($this->_connectTimeoutTimer) {
            Timer::del($this->_connectTimeoutTimer);
        }
        if ($this->_reconnectTimer) {
            Timer::del($this->_reconnectTimer);
        }
    }

    /**
     * error
     *
     * @return string
     */
    public function error()
    {
        return $this->_error;
    }

    /**
     * close
     */
    public function close()
    {
        $this->closeConnection();
        $this->_queue = [];
        gc_collect_cycles();
        if (function_exists('gc_mem_caches')) {
            gc_mem_caches();
        }
    }

    /**
     * scan
     *
     * @throws Exception
     */
    public function scan()
    {
        throw new Exception('Not implemented');
    }

    /**
     * hScan
     *
     * @throws Exception
     */
    public function hScan()
    {
        throw new Exception('Not implemented');
    }

    /**
     * hScan
     *
     * @throws Exception
     */
    public function sScan()
    {
        throw new Exception('Not implemented');
    }

    /**
     * hScan
     *
     * @throws Exception
     */
    public function zScan()
    {
        throw new Exception('Not implemented');
    }

}
