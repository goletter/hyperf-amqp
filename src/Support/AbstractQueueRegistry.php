<?php

declare(strict_types=1);

namespace Goletter\Amqp\Support;

use Goletter\Amqp\Contract\QueueRegistryInterface;
use Goletter\Amqp\Sharded\ShardedQueue;

/**
 * 应用侧注册表可继承本类，只实现 queues()。
 */
abstract class AbstractQueueRegistry implements QueueRegistryInterface
{
    /**
     * @return array<string, class-string<ShardedQueue>>
     */
    abstract protected function queues(): array;

    public function all(): array
    {
        return $this->queues();
    }

    public function classes(): array
    {
        return array_values($this->queues());
    }

    public function get(string $name): ?string
    {
        return $this->queues()[$name] ?? null;
    }
}
