<?php

declare(strict_types=1);

namespace Goletter\Amqp\Sharded;

use Hyperf\Amqp\ConnectionFactory;
use Hyperf\Amqp\Consumer;
use Hyperf\Amqp\Producer;
use Hyperf\Context\ApplicationContext;
use InvalidArgumentException;
use PhpAmqpLib\Wire\AMQPTable;

/**
 * 按业务串行键分片：同一 key 进同一队列串行，不同 key 可并行。
 *
 * 通用分片基类。租户+业务串行场景可继承 TenantBusinessShardedQueue，
 * 只需声明 EXCHANGE 与 consumerClass() / producerClass()。
 */
abstract class ShardedQueue
{
    abstract public static function exchange(): string;

    /**
     * 决定串行范围的业务键，例如 tenant_id:busine_id。
     */
    abstract public static function serialKey(array $data): string;

    /**
     * @return class-string<ShardedConsumer>
     */
    abstract public static function consumerClass(): string;

    /**
     * @return class-string<ShardedProducer>
     */
    abstract public static function producerClass(): string;

    public static function shards(): int
    {
        return 16;
    }

    /**
     * 同组分片内，本条 ACK 前等待的毫秒数（下一条才会被投递）。
     * 0 = 无额外间隔（仍由 prefetch=1 保证同片串行）。
     */
    public static function intervalMs(): int
    {
        return 0;
    }

    /**
     * 失败自动重试的最大执行次数（含首次）。超过后 DROP。
     * 若 useDeadLetter() 为 true 则进死信，否则直接丢弃。
     */
    public static function maxAttempts(): int
    {
        return 3;
    }

    /**
     * 是否挂死信交换机/队列。
     * false 时 DROP / reject(requeue=false) 直接丢弃，不声明 DLX/DLQ。
     */
    public static function useDeadLetter(): bool
    {
        return true;
    }

    /**
     * 失败后重投前等待的毫秒数（占着当前分片，同组后续消息也会等）。
     */
    public static function retryDelayMs(): int
    {
        return 2000;
    }

    public static function dlxExchange(): string
    {
        return static::exchange() . '.dlx';
    }

    public static function dlq(): string
    {
        return static::exchange() . '.dlq';
    }

    public static function dlqRoutingKey(): string
    {
        return 'dlq';
    }

    public static function shard(array $data): int
    {
        $hash = (int) sprintf('%u', crc32(static::serialKey($data)));

        return $hash % static::shards();
    }

    public static function queue(int $shard): string
    {
        return static::exchange() . '.' . $shard;
    }

    public static function routingKey(int $shard): string
    {
        return 'shard.' . $shard;
    }

    /**
     * 声明死信交换机 + 死信队列。
     * 业务分片 reject/DROP 且不 requeue 时会进入这里。
     * useDeadLetter() 为 false 时为 no-op。
     */
    public static function declareDeadLetter(string $pool = 'default'): void
    {
        if (! static::useDeadLetter()) {
            return;
        }

        $factory = ApplicationContext::getContainer()->get(ConnectionFactory::class);
        $connection = $factory->getConnection($pool);
        $channel = $connection->getChannel();
        try {
            $channel->exchange_declare(
                static::dlxExchange(),
                'direct',
                false,
                true,
                false,
                false,
                false,
                new AMQPTable(),
                null
            );

            $args = new AMQPTable();
            $args->set('x-queue-type', 'quorum');
            $channel->queue_declare(
                static::dlq(),
                false,
                true,
                false,
                false,
                false,
                $args,
                null
            );
            $channel->queue_bind(static::dlq(), static::dlxExchange(), static::dlqRoutingKey());
        } finally {
            $connection->releaseChannel($channel);
        }
    }

    /**
     * 发送到分片队列（自动算 shard、声明队列、路由）。
     *
     *   AccountRechargeQueue::produce($data);
     *
     * 自动写入 produced_at（毫秒时间戳），供消费侧算排队/端到端耗时；已有则保留（重试不覆盖）。
     */
    public static function produce(array $data): bool
    {
        $producerClass = static::producerClass();
        if (! is_a($producerClass, ShardedProducer::class, true)) {
            throw new InvalidArgumentException($producerClass . ' 必须继承 ' . ShardedProducer::class);
        }

        if (! isset($data['produced_at']) || $data['produced_at'] === '' || $data['produced_at'] === null) {
            $data['produced_at'] = (int) round(microtime(true) * 1000);
        }

        return ApplicationContext::getContainer()->get(Producer::class)->produce(new $producerClass($data));
    }

    /**
     * 发送前声明并绑定分片队列。否则交换机上没有匹配队列时，消息会被直接丢弃。
     *
     * 注意：给已有队列追加/去掉 DLX 等于改定义，需先删旧队列再建，否则 PRECONDITION_FAILED。
     */
    public static function declareShard(int $shard): void
    {
        $consumerClass = static::consumerClass();
        if (! is_a($consumerClass, ShardedConsumer::class, true)) {
            throw new InvalidArgumentException($consumerClass . ' 必须继承 ' . ShardedConsumer::class);
        }

        if (static::useDeadLetter()) {
            static::declareDeadLetter();
        }

        /** @var ShardedConsumer $message */
        $message = new $consumerClass($shard);
        $container = ApplicationContext::getContainer();
        $factory = $container->get(ConnectionFactory::class);
        $connection = $factory->getConnection($message->getPoolName());
        $channel = $connection->getChannel();
        try {
            $container->get(Consumer::class)->declare($message, $channel);
        } finally {
            $connection->releaseChannel($channel);
        }
    }
}
