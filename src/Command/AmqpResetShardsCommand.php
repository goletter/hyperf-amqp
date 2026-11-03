<?php

declare(strict_types=1);

namespace Goletter\Amqp\Command;

use Hyperf\Amqp\ConnectionFactory;
use Hyperf\Command\Command as HyperfCommand;
use PhpAmqpLib\Channel\AMQPChannel;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Throwable;

use Hyperf\Context\ApplicationContext;

/**
 * 删除旧分片队列以便按新参数（含 DLX）重建。
 *
 * RabbitMQ 不允许修改已有队列的 x-dead-letter-*，改代码后若出现
 * PRECONDITION_FAILED inequivalent arg 'x-dead-letter-exchange'，执行本命令。
 *
 *   php bin/hyperf.php amqp:reset-shards accountRecharge --force
 *   php bin/hyperf.php amqp:reset-shards --all --force
 */
class AmqpResetShardsCommand extends HyperfCommand
{
    use ResolvesShardedQueues;

    public function __construct()
    {
        parent::__construct('amqp:reset-shards');
    }

    public function configure(): void
    {
        parent::configure();
        $this->setDescription('删除分片队列（保留死信），以便用新参数重建');
        $this->addArgument('name', InputArgument::OPTIONAL, '业务名，如 accountRecharge');
        $this->addOption('all', null, InputOption::VALUE_NONE, '重置注册表内全部业务');
        $this->addOption('force', 'f', InputOption::VALUE_NONE, '确认删除（必填，防止误删）');
        $this->addOption('purge-dlq', null, InputOption::VALUE_NONE, '同时删除死信队列');
    }

    public function handle()
    {
        if (! $this->requireForce('会删除业务分片队列中未消费消息；加 --purge-dlq 时还会删除死信队列。')) {
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

        $purgeDlq = (bool) $this->input->getOption('purge-dlq');

        foreach ($queues as $alias => $queueClass) {
            $this->info('重置：' . $alias);

            try {
                $connection = $factory->getConnection('default');
                $channel = $connection->getChannel();
                try {
                    for ($shard = 0; $shard < $queueClass::shards(); ++$shard) {
                        $queue = $queueClass::queue($shard);
                        $channel = $this->deleteQueue($connection, $channel, $queue);
                    }

                    if ($purgeDlq && $queueClass::useDeadLetter()) {
                        $channel = $this->deleteQueue($connection, $channel, $queueClass::dlq());
                    }

                    $connection->releaseChannel($channel);
                    if ($queueClass::useDeadLetter()) {
                        $queueClass::declareDeadLetter();
                        $channel = $connection->getChannel();
                        $this->line('  declared ' . $queueClass::dlxExchange() . ' + ' . $queueClass::dlq());
                    } else {
                        $channel = $connection->getChannel();
                        $this->line('  skip DLX/DLQ（useDeadLetter=false）');
                    }
                } finally {
                    try {
                        $connection->releaseChannel($channel);
                    } catch (Throwable) {
                    }
                }
            } catch (Throwable $e) {
                $this->error(sprintf('[%s] 重置失败：%s', $alias, $e->getMessage()));
                $this->line('  排查：AMQP 连通性、权限；若仍报 PRECONDITION_FAILED，确认旧队列是否已删干净。');
            }
        }

        $this->info('完成。请重启服务，分片队列会按新定义（含是否挂 DLX）自动创建。');
    }

    private function deleteQueue(mixed $connection, AMQPChannel $channel, string $queue): AMQPChannel
    {
        try {
            $channel->queue_delete($queue, false, false, false);
            $this->line('  deleted ' . $queue);
            return $channel;
        } catch (Throwable $e) {
            $this->warn(sprintf('  skip %s：%s', $queue, $e->getMessage()));
            try {
                $channel->close();
            } catch (Throwable) {
            }

            return $connection->getChannel();
        }
    }
}
