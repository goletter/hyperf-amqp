<?php

declare(strict_types=1);

namespace Goletter\Amqp\Contract;

use Goletter\Amqp\Sharded\ShardedQueue;

/**
 * 分片业务队列注册表：消费进程与运维命令共用。
 */
interface QueueRegistryInterface
{
    /**
     * @return array<string, class-string<ShardedQueue>>
     */
    public function all(): array;

    /**
     * @return list<class-string<ShardedQueue>>
     */
    public function classes(): array;

    /**
     * @return null|class-string<ShardedQueue>
     */
    public function get(string $name): ?string;
}
