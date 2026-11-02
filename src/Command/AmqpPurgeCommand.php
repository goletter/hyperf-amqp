<?php

declare(strict_types=1);

namespace Goletter\Amqp\Command;

use Hyperf\Amqp\ConnectionFactory;
use Hyperf\Command\Annotation\Command;
use Hyperf\Command\Command as HyperfCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Throwable;

use Hyperf\Context\ApplicationContext;
use Goletter\Amqp\Support\AmqpLogger;

/**
 * 清空业务分片队列中的消息（保留队列定义，不删队列）。
 *
 *   php bin/hyperf.php amqp:purge accountInsight --force
 *   php bin/hyperf.php amqp:purge accountInsight --force --dlq
 *   php bin/hyperf.php amqp:purge --all --force
 */
#[Command]
class AmqpPurgeCommand extends HyperfCommand
{
    use ResolvesShardedQueues;

    public function __construct()
    {
        parent::__construct('amqp:purge');
    }

    public function configure(): void
    {
        parent::configure();
        $this->setDescription('清空分片业务队列中的消息（保留队列，不重建）');
        $this->addArgument('name', InputArgument::OPTIONAL, '业务名，如 accountInsight');
        $this->addOption('all', null, InputOption::VALUE_NONE, '清空注册表内全部业务分片队列');
        $this->addOption('force', 'f', InputOption::VALUE_NONE, '确认清空（必填，防止误删）');
        $this->addOption('dlq', null, InputOption::VALUE_NONE, '同时清空对应死信队列');
    }

    public function handle()
    {
        if (! $this->requireForce('会永久丢弃未消费的业务消息，且不可恢复。')) {
            return;
        }

        $name = $this->input->getArgument('name');
        $queues = $this->resolveShardedQueues($name === null ? null : (string) $name);
        if ($queues === []) {
            return;
        }

        $alsoDlq = (bool) $this->input->getOption('dlq');

        try {
            $factory = ApplicationContext::getContainer()->get(ConnectionFactory::class);
        } catch (Throwable $e) {
            $this->error('无法初始化 AMQP 连接工厂：' . $e->getMessage());

            return;
        }

        $totalPurged = 0;
        $failed = 0;

        foreach ($queues as $alias => $queueClass) {
            $aliasPurged = 0;

            try {
                $connection = $factory->getConnection('default');
                $channel = $connection->getChannel();
                try {
                    for ($shard = 0; $shard < $queueClass::shards(); ++$shard) {
                        $queue = $queueClass::queue($shard);
                        try {
                            $purged = (int) $channel->queue_purge($queue);
                        } catch (Throwable $e) {
                            // 队列尚不存在时跳过
                            if (str_contains($e->getMessage(), 'NOT_FOUND') || str_contains($e->getMessage(), '404')) {
                                $this->line(sprintf('  skip %s（不存在）', $queue));
                                continue;
                            }
                            throw $e;
                        }
                        $aliasPurged += $purged;
                        if ($purged > 0) {
                            $this->line(sprintf('  %s purged=%d', $queue, $purged));
                        }
                    }

                    if ($alsoDlq) {
                        if (! $queueClass::useDeadLetter()) {
                            $this->line('  skip dlq（useDeadLetter=false）');
                        } else {
                            $dlq = $queueClass::dlq();
                            try {
                                $queueClass::declareDeadLetter();
                                $purged = (int) $channel->queue_purge($dlq);
                                $aliasPurged += $purged;
                                $this->line(sprintf('  %s purged=%d', $dlq, $purged));
                            } catch (Throwable $e) {
                                if (! str_contains($e->getMessage(), 'NOT_FOUND') && ! str_contains($e->getMessage(), '404')) {
                                    throw $e;
                                }
                                $this->line(sprintf('  skip %s（不存在）', $dlq));
                            }
                        }
                    }
                } finally {
                    try {
                        $connection->releaseChannel($channel);
                    } catch (Throwable) {
                    }
                }
            } catch (Throwable $e) {
                ++$failed;
                $this->error(sprintf('[%s] 清空失败：%s', $alias, $e->getMessage()));
                $this->line('  排查：AMQP 连通性、队列是否存在、账号是否有 purge 权限。');
                continue;
            }

            $totalPurged += $aliasPurged;
            $this->info(sprintf('[%s] 共清空 %d 条', $alias, $aliasPurged));

            if ($aliasPurged > 0) {
                AmqpLogger::log([
                    'event' => 'shards_purged',
                    'alias' => $alias,
                    'purged' => $aliasPurged,
                    'dlq' => $alsoDlq,
                    'shards' => $queueClass::shards(),
                ], '分片业务队列已清空', 'queue');
            }
        }

        if ($failed > 0) {
            $this->warn(sprintf('完成：清空 %d 条，失败业务 %d 个', $totalPurged, $failed));

            return;
        }

        $this->info(sprintf('完成：共清空 %d 条消息', $totalPurged));
    }
}
