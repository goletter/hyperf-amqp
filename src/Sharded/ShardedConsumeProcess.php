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

use Hyperf\Amqp\Consumer;
use Hyperf\Coroutine\Coroutine;
use Hyperf\Process\AbstractProcess;
use Hyperf\Process\ProcessManager;
use InvalidArgumentException;
use Throwable;

/**
 * 为若干分片队列拉起消费协程（每个队列 × 每个分片一条）。
 *
 * 子类实现 queues()，并在 config/autoload/processes.php 注册（不要再用 #[Process]，防双重启动）。
 */
abstract class ShardedConsumeProcess extends AbstractProcess
{
    public function handle(): void
    {
        $consumer = $this->container->get(Consumer::class);

        foreach ($this->queues() as $queueClass) {
            if (! is_a($queueClass, ShardedQueue::class, true)) {
                throw new InvalidArgumentException($queueClass . ' 必须继承 ' . ShardedQueue::class);
            }

            $consumerClass = $queueClass::consumerClass();
            if (! is_a($consumerClass, ShardedConsumer::class, true)) {
                throw new InvalidArgumentException($consumerClass . ' 必须继承 ' . ShardedConsumer::class);
            }

            // 启用死信时先建 DLX/DLQ，再拉起分片消费（声明业务队列时会挂上死信参数）
            if ($queueClass::useDeadLetter()) {
                $queueClass::declareDeadLetter();
            }

            for ($shard = 0; $shard < $queueClass::shards(); ++$shard) {
                Coroutine::create(function () use ($consumer, $consumerClass, $shard) {
                    while (ProcessManager::isRunning()) {
                        try {
                            $consumer->consume(new $consumerClass($shard));
                        } catch (Throwable $e) {
                            $this->logThrowable($e);
                        }

                        if (! ProcessManager::isRunning()) {
                            return;
                        }

                        Coroutine::sleep(1);
                    }
                });
            }
        }

        while (ProcessManager::isRunning()) {
            Coroutine::sleep(1);
        }
    }

    /**
     * @return list<class-string<ShardedQueue>>
     */
    abstract protected function queues(): array;
}
