<?php

declare(strict_types=1);

namespace Goletter\Amqp;

use Goletter\Amqp\Command\AmqpDlqInfoCommand;
use Goletter\Amqp\Command\AmqpDlqPurgeCommand;
use Goletter\Amqp\Command\AmqpDlqReplayCommand;
use Goletter\Amqp\Command\AmqpMakeQueueCommand;
use Goletter\Amqp\Command\AmqpPurgeCommand;
use Goletter\Amqp\Command\AmqpResetShardsCommand;

class ConfigProvider
{
    public function __invoke(): array
    {
        return [
            'dependencies' => [
            ],
            'commands' => [
                AmqpDlqInfoCommand::class,
                AmqpDlqPurgeCommand::class,
                AmqpDlqReplayCommand::class,
                AmqpMakeQueueCommand::class,
                AmqpPurgeCommand::class,
                AmqpResetShardsCommand::class,
            ],
            'annotations' => [
                'scan' => [
                    'paths' => [
                        __DIR__,
                    ],
                ],
            ],
            'publish' => [
                [
                    'id' => 'config',
                    'description' => '分片 AMQP 队列生成路径等配置。',
                    'source' => __DIR__ . '/../publish/amqp_sharded.php',
                    'destination' => BASE_PATH . '/config/autoload/amqp_sharded.php',
                ],
            ],
        ];
    }
}
