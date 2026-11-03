<?php

declare(strict_types=1);

namespace Goletter\Amqp\Command;

use Hyperf\Amqp\ConnectionFactory;
use Hyperf\Command\Command as HyperfCommand;
use Hyperf\Context\ApplicationContext;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Throwable;

/**
 * 查看分片业务队列积压（Ready）与消费者数。
 *
 *   php bin/hyperf.php amqp:info
 *   php bin/hyperf.php amqp:info accountInsight
 *   php bin/hyperf.php amqp:info accountInsight --verbose-shards
 */
class AmqpInfoCommand extends HyperfCommand
{
    use ResolvesShardedQueues;

    public function __construct()
    {
        parent::__construct('amqp:info');
    }

    public function configure(): void
    {
        parent::configure();
        $this->setDescription('查看分片业务队列积压与消费者数');
        $this->addArgument('name', InputArgument::OPTIONAL, '业务名；省略则看全部');
        $this->addOption('all', null, InputOption::VALUE_NONE, '检查注册表内全部队列');
        $this->addOption('verbose-shards', null, InputOption::VALUE_NONE, '打印每个分片明细（默认只打印有积压的分片）');
    }

    public function handle()
    {
        $name = $this->input->getArgument('name');
        $queues = $this->resolveShardedQueues(
            $name === null ? null : (string) $name,
            allowAllWhenEmpty: true
        );
        if ($queues === []) {
            return;
        }

        $verbose = (bool) $this->input->getOption('verbose-shards');

        try {
            $factory = ApplicationContext::getContainer()->get(ConnectionFactory::class);
        } catch (Throwable $e) {
            $this->error('无法初始化 AMQP 连接工厂：' . $e->getMessage());

            return;
        }

        $grandReady = 0;
        $grandConsumers = 0;

        foreach ($queues as $alias => $queueClass) {
            $ready = 0;
            $consumers = 0;
            $missing = 0;
            $shardRows = [];

            try {
                $connection = $factory->getConnection('default');
                $channel = $connection->getChannel();
                try {
                    for ($shard = 0; $shard < $queueClass::shards(); ++$shard) {
                        $queue = $queueClass::queue($shard);
                        try {
                            [, $messageCount, $consumerCount] = $channel->queue_declare(
                                $queue,
                                true,
                                true,
                                false,
                                false,
                                false,
                                [],
                                null
                            );
                            $messageCount = (int) $messageCount;
                            $consumerCount = (int) $consumerCount;
                            $ready += $messageCount;
                            $consumers += $consumerCount;
                            if ($verbose || $messageCount > 0) {
                                $shardRows[] = sprintf(
                                    '  shard=%d queue=%s ready=%d consumers=%d',
                                    $shard,
                                    $queue,
                                    $messageCount,
                                    $consumerCount
                                );
                            }
                        } catch (Throwable $e) {
                            ++$missing;
                            if ($verbose) {
                                $shardRows[] = sprintf('  shard=%d queue=%s missing (%s)', $shard, $queue, $e->getMessage());
                            }
                        }
                    }

                    $dlqReady = null;
                    if ($queueClass::useDeadLetter()) {
                        try {
                            [, $dlqCount] = $channel->queue_declare(
                                $queueClass::dlq(),
                                true,
                                true,
                                false,
                                false,
                                false,
                                [],
                                null
                            );
                            $dlqReady = (int) $dlqCount;
                        } catch (Throwable) {
                            $dlqReady = null;
                        }
                    }
                } finally {
                    try {
                        $connection->releaseChannel($channel);
                    } catch (Throwable) {
                    }
                }
            } catch (Throwable $e) {
                $this->error(sprintf('[%s] 查询失败：%s', $alias, $e->getMessage()));
                continue;
            }

            $grandReady += $ready;
            $grandConsumers += $consumers;

            $dlqPart = $dlqReady === null
                ? 'dlq=off'
                : 'dlq_ready=' . $dlqReady;

            $this->info(sprintf(
                '[%s] shards=%d ready=%d consumers=%d missing=%d %s serial=%s',
                $alias,
                $queueClass::shards(),
                $ready,
                $consumers,
                $missing,
                $dlqPart,
                implode('+', $this->serialFieldsOf($queueClass))
            ));

            foreach ($shardRows as $row) {
                $this->line($row);
            }
        }

        $this->line(sprintf('合计 ready=%d consumers=%d', $grandReady, $grandConsumers));
    }

    /**
     * @param class-string $queueClass
     * @return list<string>
     */
    private function serialFieldsOf(string $queueClass): array
    {
        if (method_exists($queueClass, 'serialFields')) {
            /** @var list<string> $fields */
            $fields = $queueClass::serialFields();

            return $fields;
        }

        return ['(custom serialKey)'];
    }
}
