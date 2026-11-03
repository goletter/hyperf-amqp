<?php

declare(strict_types=1);

namespace Goletter\Amqp\Command;

use Goletter\Amqp\Support\AmqpLogger;
use Hyperf\Amqp\ConnectionFactory;
use Hyperf\Command\Command as HyperfCommand;
use Hyperf\Context\ApplicationContext;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Throwable;

/**
 * 清空死信队列消息（保留队列定义）。
 *
 *   php bin/hyperf.php amqp:dlq-purge accountRecharge --force
 *   php bin/hyperf.php amqp:dlq-purge --all --force
 */
class AmqpDlqPurgeCommand extends HyperfCommand
{
    use ResolvesShardedQueues;

    public function __construct()
    {
        parent::__construct('amqp:dlq-purge');
    }

    public function configure(): void
    {
        parent::configure();
        $this->setDescription('清空分片业务死信队列中的消息');
        $this->addArgument('name', InputArgument::OPTIONAL, '业务名，如 accountRecharge');
        $this->addOption('all', null, InputOption::VALUE_NONE, '清空注册表内全部死信队列');
        $this->addOption('force', 'f', InputOption::VALUE_NONE, '确认清空（必填，防止误删）');
    }

    public function handle()
    {
        if (! $this->requireForce('会永久丢弃死信消息，且不可恢复（不会重投回业务队列）。')) {
            return;
        }

        $name = $this->input->getArgument('name');
        $queues = $this->resolveShardedQueues($name === null ? null : (string) $name);
        if ($queues === []) {
            return;
        }

        try {
            $factory = ApplicationContext::getContainer()->get(ConnectionFactory::class);
        } catch (Throwable $e) {
            $this->error('无法初始化 AMQP 连接工厂：' . $e->getMessage());
            return;
        }

        $totalPurged = 0;
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
                    $purged = (int) $channel->queue_purge($dlq);
                } finally {
                    try {
                        $connection->releaseChannel($channel);
                    } catch (Throwable) {
                    }
                }
            } catch (Throwable $e) {
                ++$failed;
                $this->error(sprintf(
                    '[%s] 清空失败 queue=%s：%s',
                    $alias,
                    $dlq,
                    $e->getMessage()
                ));
                $this->line('  排查：检查 AMQP 连通性、队列是否存在、账号是否有 purge 权限。');
                continue;
            }

            $totalPurged += $purged;
            $this->line(sprintf('[%s] %s purged=%d', $alias, $dlq, $purged));

            if ($purged > 0) {
                AmqpLogger::log([
                    'event' => 'dlq_purged',
                    'alias' => $alias,
                    'queue' => $dlq,
                    'purged' => $purged,
                ], '分片队列死信已清空', 'queue');
            }
        }

        if ($failed > 0) {
            $this->warn(sprintf('完成：清空 %d 条，失败业务 %d 个', $totalPurged, $failed));
            return;
        }

        $this->info(sprintf('完成：共清空 %d 条死信消息', $totalPurged));
    }
}
