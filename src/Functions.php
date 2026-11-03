<?php

declare(strict_types=1);

use Hyperf\Amqp\Message\ProducerMessage;
use Hyperf\Amqp\Producer;
use Hyperf\Context\ApplicationContext;

if (! function_exists('produce')) {
    /**
     * 推送消息到 AMQP.
     */
    function produce(ProducerMessage $message): bool
    {
        return ApplicationContext::getContainer()->get(Producer::class)->produce($message);
    }
}
