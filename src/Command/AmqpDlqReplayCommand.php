<?php

declare(strict_types=1);

namespace Goletter\Amqp\Command;

use Goletter\Amqp\Sharded\ShardedProducer;
use Goletter\Amqp\Sharded\ShardedQueue;
use Hyperf\Amqp\ConnectionFactory;
use Hyperf\Amqp\Packer\Packer;
use Hyperf\Command\Command as HyperfCommand;
use PhpAmqpLib\Message\AMQPMessage;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Throwable;
use Hyperf\Context\ApplicationContext;

/**
 * 将死信消息重投回业务分片队列（重置 attempts）。
 *
 *   php bin/hyperf.php amqp:dlq-replay accountRecharge --limit=10
 *   php bin/hyperf.php amqp:dlq-replay accountRecharge --dry-run
 *   php bin/hyperf.php amqp:dlq-replay accountRecharge --where=account_id:5 --scan=200
 *   php bin/hyperf.php amqp:dlq-replay accountRecharge --where=tenant_id:1 --where=account_id:5
 */
class AmqpDlqReplayCommand extends HyperfCommand
{
    use ResolvesShardedQueues;

    public function __construct()
    {
        parent::__construct('amqp:dlq-replay');
    }

    public function configure(): void
    {
        parent::configure();
        $this->setDescription('从死信队列重投消息到业务分片队列');
        $this->addArgument('name', InputArgument::REQUIRED, '业务名，如 accountRecharge');
        $this->addOption('limit', null, InputOption::VALUE_REQUIRED, '最多重投条数（有 --where 时指匹配条数）', '10');
        $this->addOption('scan', null, InputOption::VALUE_REQUIRED, '有 --where 时最多扫描死信条数', '500');
        $this->addOption('where', null, InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY, '按 payload 字段过滤，格式 field:value，可多次');
        $this->addOption('dry-run', null, InputOption::VALUE_NONE, '只打印不真正投递/ACK');
    }

    public function handle()
    {
        $name = (string) $this->input->getArgument('name');
        $queues = $this->resolveShardedQueues($name);
        if ($queues === []) {
            return;
        }
        /** @var class-string<ShardedQueue> $queueClass */
        $queueClass = $queues[$name];

        if (! $queueClass::useDeadLetter()) {
            $this->error(sprintf('业务 %s 未启用死信（useDeadLetter=false），无法重投。', $name));
            return;
        }

        $producerClass = $queueClass::producerClass();
        if (! is_a($producerClass, ShardedProducer::class, true)) {
            $this->error(sprintf(
                '业务 %s 的 producerClass=%s 必须继承 ShardedProducer，无法按分片重投。',
                $name,
                $producerClass
            ));
            return;
        }

        try {
            $filters = $this->parseWhere((array) $this->input->getOption('where'));
        } catch (Throwable $e) {
            $this->error('参数 --where 无效：' . $e->getMessage());
            $this->line('正确格式：--where=account_id:5  （可重复传入多个条件，为 AND）');
            return;
        }

        $limit = max(1, (int) $this->input->getOption('limit'));
        $scan = max($limit, (int) $this->input->getOption('scan'));
        $dryRun = (bool) $this->input->getOption('dry-run');
        $pull = $filters === [] ? $limit : $scan;

        try {
            $queueClass::declareDeadLetter();
            $factory = ApplicationContext::getContainer()->get(ConnectionFactory::class);
            $packer = ApplicationContext::getContainer()->get(Packer::class);
            $connection = $factory->getConnection('default');
            $channel = $connection->getChannel();
        } catch (Throwable $e) {
            $this->error('连接 RabbitMQ / 声明死信失败：' . $e->getMessage());
            $this->line('排查：检查 .env 中 AMQP_*、Broker 是否可达、死信队列是否已创建。');
            return;
        }

        $ok = 0;
        $fail = 0;
        $skipped = 0;

        try {
            /** @var list<array{message: AMQPMessage, data: array}> $matched */
            $matched = [];
            /** @var list<AMQPMessage> $unmatched */
            $unmatched = [];

            for ($i = 0; $i < $pull; ++$i) {
                $message = $channel->basic_get($queueClass::dlq(), false);
                if ($message === null) {
                    break;
                }

                try {
                    $data = $packer->unpack($message->getBody());
                    if (! is_array($data)) {
                        throw new \RuntimeException('payload 不是数组');
                    }
                } catch (Throwable $e) {
                    ++$fail;
                    $this->error('解析失败，放回死信：' . $e->getMessage());
                    $unmatched[] = $message;
                    continue;
                }

                if ($filters !== [] && ! $this->matches($data, $filters)) {
                    $unmatched[] = $message;
                    ++$skipped;
                    continue;
                }

                $matched[] = ['message' => $message, 'data' => $data];
                if (count($matched) >= $limit) {
                    // 已凑够要重投的条数，剩余还在队列里未取出
                    break;
                }
            }

            // 不匹配的先放回，避免占用 unacked
            foreach ($unmatched as $message) {
                $channel->basic_nack($message->getDeliveryTag(), false, true);
            }

            foreach ($matched as $index => $item) {
                $message = $item['message'];
                $data = $item['data'];
                try {
                    unset($data['attempts']);

                    $shard = $queueClass::shard($data);
                    $this->line(sprintf(
                        '#%d key=%s payload=%s -> %s %s',
                        $index + 1,
                        $queueClass::serialKey($data),
                        $this->previewPayload($data, $filters),
                        $queueClass::queue($shard),
                        $dryRun ? '[dry-run]' : ''
                    ));

                    if ($dryRun) {
                        $channel->basic_nack($message->getDeliveryTag(), false, true);
                    } else {
                        $queueClass::produce($data);
                        $channel->basic_ack($message->getDeliveryTag());
                    }
                    ++$ok;
                } catch (Throwable $e) {
                    ++$fail;
                    $this->error('重投失败：' . $e->getMessage());
                    $channel->basic_nack($message->getDeliveryTag(), false, true);
                }
            }
        } finally {
            $connection->releaseChannel($channel);
        }

        $this->info(sprintf(
            '完成：重投 %d，失败 %d，跳过(不匹配) %d%s',
            $ok,
            $fail,
            $skipped,
            $dryRun ? '（dry-run 未真正投递）' : ''
        ));
    }

    /**
     * @param list<string> $wheres
     * @return array<string, string>
     */
    private function parseWhere(array $wheres): array
    {
        $filters = [];
        foreach ($wheres as $where) {
            $where = trim((string) $where);
            if ($where === '') {
                continue;
            }
            $pos = strpos($where, ':');
            if ($pos === false || $pos === 0) {
                throw new \InvalidArgumentException('无效 --where，格式应为 field:value，例如 account_id:5');
            }
            $field = trim(substr($where, 0, $pos));
            $value = substr($where, $pos + 1);
            if ($field === '') {
                throw new \InvalidArgumentException('无效 --where：字段名为空');
            }
            $filters[$field] = $value;
        }

        return $filters;
    }

    /**
     * @param array<string, mixed> $data
     * @param array<string, string> $filters
     */
    private function matches(array $data, array $filters): bool
    {
        foreach ($filters as $field => $expected) {
            if (! array_key_exists($field, $data)) {
                return false;
            }
            if ((string) $data[$field] !== (string) $expected) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param array<string, mixed> $data
     * @param array<string, string> $filters
     */
    private function previewPayload(array $data, array $filters): string
    {
        $keys = $filters !== [] ? array_keys($filters) : ['account_id', 'tenant_id', 'busine_id', 'status'];
        $parts = [];
        foreach ($keys as $key) {
            if (array_key_exists($key, $data)) {
                $parts[] = $key . '=' . $data[$key];
            }
        }

        return $parts === [] ? '{}' : implode(',', $parts);
    }
}
