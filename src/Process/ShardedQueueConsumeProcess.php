<?php

declare(strict_types=1);

namespace Goletter\Amqp\Process;

use Goletter\Amqp\Contract\QueueRegistryInterface;
use Goletter\Amqp\Sharded\ShardedConsumeProcess;

/**
 * 包内通用分片消费进程：消费 QueueRegistryInterface 注册的全部队列。
 *
 * 在 config/autoload/processes.php 注册本类即可，勿再加 #[Process]。
 */
class ShardedQueueConsumeProcess extends ShardedConsumeProcess
{
    public string $name = 'sharded-queue-consume';

    public int $nums = 1;

    protected function queues(): array
    {
        return $this->container->get(QueueRegistryInterface::class)->classes();
    }
}
