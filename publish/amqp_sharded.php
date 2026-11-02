<?php

declare(strict_types=1);

/**
 * goletter/hyperf-amqp 分片队列配置。
 *
 * 仅供 amqp:make-queue 代码生成使用，不参与消费 / 发消息 / 死信等运行时逻辑。
 *
 * 队列列表不在这里：运行时靠应用绑定的 QueueRegistryInterface
 *（例如 AccountQueueRegistry）。
 *
 * 本文件只规定脚手架「生成到哪、注册表写哪个文件」。
 * 执行 php bin/hyperf.php amqp:make-queue accountRefund 时：
 * - 按下方 namespace / path 生成 Queue、Producer、Consumer 三件套
 * - 在 registry_file 的 all() 中追加 'accountRefund' => AccountRefundQueue::class
 *
 * 若项目目录结构变化，只改本配置即可，无需改包代码。
 */
return [
    'generate' => [
        /** Queue 类的 PHP 命名空间 */
        'queue_namespace' => 'App\\Amqp\\Queue',
        /** Producer 类的 PHP 命名空间 */
        'producer_namespace' => 'App\\Amqp\\Producer',
        /** Consumer 类的 PHP 命名空间 */
        'consumer_namespace' => 'App\\Amqp\\Consumer',
        /** Queue 文件落盘目录 */
        'queue_path' => BASE_PATH . '/app/Amqp/Queue',
        /** Producer 文件落盘目录 */
        'producer_path' => BASE_PATH . '/app/Amqp/Producer',
        /** Consumer 文件落盘目录 */
        'consumer_path' => BASE_PATH . '/app/Amqp/Consumer',
        /** 生成后往哪个注册表文件追加一行（通常即 QueueRegistry 实现类） */
        'registry_file' => BASE_PATH . '/app/Amqp/Queue/AccountQueueRegistry.php',
    ],
];
