<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */

namespace Goletter\Amqp\Sharded;

use Hyperf\Amqp\Message\ProducerMessage;
use InvalidArgumentException;

/**
 * 按 ShardedQueue 自动路由到分片。
 *
 * 子类实现 queueClass()，并保留 #[Producer(exchange: XxxQueue::EXCHANGE)] 注解。
 */
abstract class ShardedProducer extends ProducerMessage
{
    public function __construct(array $data)
    {
        $queueClass = static::queueClass();
        if (! is_a($queueClass, ShardedQueue::class, true)) {
            throw new InvalidArgumentException($queueClass . ' 必须继承 ' . ShardedQueue::class);
        }

        $this->payload = $data;
        $shard = $queueClass::shard($data);
        $this->setExchange($queueClass::exchange());
        $this->setRoutingKey($queueClass::routingKey($shard));
        $queueClass::declareShard($shard);
    }

    /**
     * @return class-string<ShardedQueue>
     */
    abstract protected static function queueClass(): string;
}
