<?php

declare(strict_types=1);

namespace Goletter\Amqp\Support;

use Hyperf\Context\ApplicationContext;
use Hyperf\Logger\LoggerFactory;
use Throwable;

/**
 * 优先走 Goletter\Utils\logging；否则回落到 LoggerFactory。
 */
final class AmqpLogger
{
    public static function log(array $context, string $message, string $group = 'queue'): void
    {
        if (function_exists('Goletter\\Utils\\logging')) {
            \Goletter\Utils\logging($context, $message, $group);
            return;
        }

        try {
            ApplicationContext::getContainer()
                ->get(LoggerFactory::class)
                ->get($group)
                ->info($message, $context);
        } catch (Throwable) {
        }
    }
}
