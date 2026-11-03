<?php

declare(strict_types=1);

namespace Goletter\Amqp\Command;

use Hyperf\Amqp\ConnectionFactory;
use Hyperf\Command\Command as HyperfCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Throwable;

use Hyperf\Context\ApplicationContext;
use Goletter\Amqp\Support\AmqpLogger;

/**
 * 查看死信积压（可挂 crontab 做定时告警）。
 *
 *   php bin/hyperf.php amqp:dlq-info
 *   php bin/hyperf.php amqp:dlq-info accountRecharge
 *   php bin/hyperf.php amqp:dlq-info --all
 */
class AmqpDlqInfoCommand extends HyperfCommand
{
    use ResolvesShardedQueues;

    public function __construct()
    {
        parent::__construct('amqp:dlq-info');
    }

    public function configure(): void
    {
        parent::configure();
        $this->setDescription('查看分片业务死信队列积压');
        $this->addArgument('name', InputArgument::OPTIONAL, '业务名，如 accountRecharge；省略则看全部');
        $this->addOption('all', null, InputOption::VALUE_NONE, '检查注册表内全部队列');
    }

    public function handle()
    {
        $name = $this->input->getArgument('name');
        // 省略 name 时默认看全部，便于 crontab
        $queues = $this->resolveShardedQueues(
            $name === null ? null : (string) $name,
            allowAllWhenEmpty: true
        );
        if ($queues === []) {
            return;
        }

        try {
            $factory = ApplicationContext::getContainer()->get(ConnectionFactory::class);
        } catch (Throwable $e) {
            $this->error('无法初始化 AMQP 连接工厂：' . $e->getMessage());
            return;
        }

        $totalReady = 0;
        $failed = 0;

        foreach ($queues as $alias => $queueClass) {
            if (! $queueClass::useDeadLetter()) {
                $this->line(sprintf('[%s] skip（useDeadLetter=false）', $alias));
                continue;
            }

            $dlq = $queueClass::dlq();
            try {
                $queueClass::declareDeadLetter();
                $connection = $factory->getConnection('default');
                $channel = $connection->getChannel();
                try {
                    [, $messageCount, $consumerCount] = $channel->queue_declare(
                        $dlq,
                        true,
                        true,
                        false,
                        false,
                        false,
                        [],
                        null
                    );
                } finally {
                    $connection->releaseChannel($channel);
                }
            } catch (Throwable $e) {
                ++$failed;
                $this->error(sprintf('[%s] 查询失败 queue=%s：%s', $alias, $dlq, $e->getMessage()));
                continue;
            }

            $messageCount = (int) $messageCount;
            $consumerCount = (int) $consumerCount;
            $totalReady += $messageCount;

            $this->line(sprintf(
                '[%s] %s ready=%d consumers=%d',
                $alias,
                $dlq,
                $messageCount,
                $consumerCount
            ));

            if ($messageCount > 0) {
                AmqpLogger::log([
                    'event' => 'dlq_backlog',
                    'alias' => $alias,
                    'queue' => $dlq,
                    'ready' => $messageCount,
                    'consumers' => $consumerCount,
                ], '分片队列死信积压', 'queue');
            }
        }

        if ($failed > 0) {
            $this->warn(sprintf('部分业务查询失败 %d 个；当前可见积压 %d 条', $failed, $totalReady));
        } elseif ($totalReady > 0) {
            $this->warn(sprintf(
                '合计死信积压 %d 条。重投：php bin/hyperf.php amqp:dlq-replay <业务名>；清空：php bin/hyperf.php amqp:dlq-purge <业务名> --force',
                $totalReady
            ));
        } else {
            $this->info('无死信积压');
        }
    }
}
