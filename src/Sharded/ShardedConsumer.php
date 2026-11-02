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

use Hyperf\Amqp\Builder\QueueBuilder;
use Hyperf\Amqp\Message\ConsumerMessage;
use Hyperf\Amqp\Message\Type;
use Hyperf\Amqp\Result;
use InvalidArgumentException;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPAbstractCollection;
use PhpAmqpLib\Wire\AMQPTable;
use Throwable;

use Goletter\Amqp\Support\AmqpLogger;

/**
 * 分片消费者：quorum + 单活跃消费者 + prefetch=1。
 *
 * 子类实现 queueClass() 和 handle()。
 * 不要加 #[Consumer]，由 ShardedConsumeProcess 按分片拉起。
 */
abstract class ShardedConsumer extends ConsumerMessage
{
    protected string|Type $type = Type::TOPIC;

    protected ?array $qos = [
        'prefetch_size' => 0,
        'prefetch_count' => 1,
        'global' => false,
    ];

    public function __construct(protected int $shard)
    {
        $queueClass = static::queueClass();
        if (! is_a($queueClass, ShardedQueue::class, true)) {
            throw new InvalidArgumentException($queueClass . ' 必须继承 ' . ShardedQueue::class);
        }

        $this->setExchange($queueClass::exchange());
        $this->setRoutingKey($queueClass::routingKey($this->shard));
        $this->setQueue($queueClass::queue($this->shard));
    }

    public function getShard(): int
    {
        return $this->shard;
    }

    /**
     * 业务处理放在 handle()。
     * - 成功 ACK：按 intervalMs 间隔后再确认
     * - handle 抛异常：按 attempts / maxAttempts 延迟重投；超限 DROP
     * - handle 主动返回 DROP：不重试.
     * @param mixed $data
     */
    final public function consumeMessage($data, AMQPMessage $message): Result
    {
        $queueClass = static::queueClass();
        $payload = is_array($data) ? $data : [];
        $startedAt = microtime(true);

        try {
            $result = $this->handle($data, $message);
        } catch (Throwable $e) {
            $this->logTiming($queueClass, $payload, $startedAt, 'exception');
            return $this->retryOrDrop($payload, $e);
        }

        $this->logTiming($queueClass, $payload, $startedAt, $result->value ?? (string) $result);

        // prefetch=1：ACK 前等待，同组分片下一条才会被投递
        $intervalMs = max(0, $queueClass::intervalMs());
        if ($intervalMs > 0 && $result === Result::ACK) {
            usleep($intervalMs * 1000);
        }

        return $result;
    }

    /**
     * 排队 / 处理 / 端到端耗时（依赖 produce 写入的 produced_at 毫秒戳）.
     *
     * @param class-string<ShardedQueue> $queueClass
     */
    protected function logTiming(string $queueClass, array $payload, float $startedAt, string $result): void
    {
        $doneAt = microtime(true);
        $handleMs = (int) round(($doneAt - $startedAt) * 1000);
        $producedAt = (int) ($payload['produced_at'] ?? 0);

        $waitMs = null;
        $totalMs = null;
        if ($producedAt > 0) {
            $startMs = (int) round($startedAt * 1000);
            $doneMs = (int) round($doneAt * 1000);
            $waitMs = max(0, $startMs - $producedAt);
            $totalMs = max(0, $doneMs - $producedAt);
        }

        $key = null;
        try {
            $key = $queueClass::serialKey($payload);
        } catch (Throwable) {
        }

        AmqpLogger::log([
            'event' => 'consume_timing',
            'exchange' => $queueClass::exchange(),
            'shard' => $this->getShard(),
            'key' => $key,
            'result' => $result,
            'wait_ms' => $waitMs,
            'handle_ms' => $handleMs,
            'total_ms' => $totalMs,
            'produced_at' => $producedAt > 0 ? $producedAt : null,
            'attempts' => (int) ($payload['attempts'] ?? 0),
        ], '分片队列消费耗时', 'queue');
    }

    public function getQueueBuilder(): QueueBuilder
    {
        $queueClass = static::queueClass();
        $arguments = new AMQPTable();
        $arguments->set('x-queue-type', 'quorum');
        // 多实例部署时，同一分片只有一个消费者在干活
        $arguments->set('x-single-active-consumer', true, AMQPAbstractCollection::T_BOOL);
        if ($queueClass::useDeadLetter()) {
            // DROP / reject(requeue=false) → 进死信
            $arguments->set('x-dead-letter-exchange', $queueClass::dlxExchange());
            $arguments->set('x-dead-letter-routing-key', $queueClass::dlqRoutingKey());
        }

        return (new QueueBuilder())
            ->setQueue($this->getQueue())
            ->setArguments($arguments);
    }

    /**
     * 失败重试：payload.attempts 计数，未超限则延迟后 produce 新消息并 ACK 当前条。
     */
    protected function retryOrDrop(array $payload, Throwable $e): Result
    {
        $queueClass = static::queueClass();
        $attempts = (int) ($payload['attempts'] ?? 0) + 1;
        $maxAttempts = max(1, $queueClass::maxAttempts());
        $useDeadLetter = $queueClass::useDeadLetter();

        AmqpLogger::log([
            'event' => 'retry',
            'exchange' => $queueClass::exchange(),
            'shard' => $this->getShard(),
            'key' => $queueClass::serialKey($payload),
            'attempts' => $attempts,
            'max_attempts' => $maxAttempts,
            'exception' => $e::class,
            'error' => $e->getMessage(),
            ...$payload,
        ], '分片队列重试', 'queue');

        if ($attempts >= $maxAttempts) {
            AmqpLogger::log([
                'event' => 'retry_exhausted',
                'exchange' => $queueClass::exchange(),
                'use_dead_letter' => $useDeadLetter,
                'dlq' => $useDeadLetter ? $queueClass::dlq() : null,
                'shard' => $this->getShard(),
                'key' => $queueClass::serialKey($payload),
                'attempts' => $attempts,
                'exception' => $e::class,
                'error' => $e->getMessage(),
            ], $useDeadLetter ? '分片队列重试耗尽，将进入死信' : '分片队列重试耗尽，直接丢弃', 'queue');

            return Result::DROP;
        }

        $delayMs = max(0, $queueClass::retryDelayMs());
        if ($delayMs > 0) {
            usleep($delayMs * 1000);
        }

        $payload['attempts'] = $attempts;
        $queueClass::produce($payload);

        return Result::ACK;
    }

    abstract protected function handle(mixed $data, AMQPMessage $message): Result;

    /**
     * @return class-string<ShardedQueue>
     */
    abstract protected static function queueClass(): string;
}
