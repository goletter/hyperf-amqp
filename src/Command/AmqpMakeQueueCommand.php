<?php

declare(strict_types=1);

namespace Goletter\Amqp\Command;

use Hyperf\Command\Command as HyperfCommand;
use Hyperf\Context\ApplicationContext;
use Hyperf\Contract\ConfigInterface;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;

/**
 * 一键生成分片队列三件套，并写入应用侧队列注册表。
 *
 *   php bin/hyperf.php amqp:make-queue accountRefund
 *   php bin/hyperf.php amqp:make-queue accountRefund --label=账户退款
 *   php bin/hyperf.php amqp:make-queue accountRefund --serial=tenant_id,account_id
 *   php bin/hyperf.php amqp:make-queue accountRefund --shards=32 --max-attempts=1 --no-dlq
 *   php bin/hyperf.php amqp:make-queue accountRefund --force
 */
class AmqpMakeQueueCommand extends HyperfCommand
{
    public function __construct()
    {
        parent::__construct('amqp:make-queue');
    }

    public function configure(): void
    {
        parent::configure();
        $this->setDescription('生成分片 Queue/Producer/Consumer，并注册到应用侧队列注册表');
        $this->addArgument('name', InputArgument::REQUIRED, '业务名，如 accountRefund / AccountRefund');
        $this->addOption('label', null, InputOption::VALUE_REQUIRED, '中文说明（注释与日志用）');
        $this->addOption('serial', null, InputOption::VALUE_REQUIRED, '串行字段，逗号分隔，默认 tenant_id,busine_id');
        $this->addOption('shards', null, InputOption::VALUE_REQUIRED, '分片数，默认不生成（用基类 16）');
        $this->addOption('max-attempts', null, InputOption::VALUE_REQUIRED, '最大执行次数（含首次），默认不生成（用基类 3）');
        $this->addOption('no-dlq', null, InputOption::VALUE_NONE, '生成 useDeadLetter(): false');
        $this->addOption('force', 'f', InputOption::VALUE_NONE, '已存在时覆盖');
        $this->addOption('no-register', null, InputOption::VALUE_NONE, '不写入队列注册表');
    }

    public function handle()
    {
        $raw = (string) $this->input->getArgument('name');
        [$studly, $exchange] = $this->normalizeName($raw);
        if ($studly === '' || $exchange === '') {
            $this->error('无效名称：' . $raw);
            return;
        }

        $label = (string) ($this->input->getOption('label') ?: $exchange);
        $force = (bool) $this->input->getOption('force');
        $serialFields = $this->parseSerialFields(
            (string) ($this->input->getOption('serial') ?: 'tenant_id,busine_id')
        );
        if ($serialFields === []) {
            $this->error('--serial 不能为空，例如 tenant_id,busine_id');
            return;
        }

        $shardsOpt = $this->input->getOption('shards');
        $shards = $shardsOpt !== null && $shardsOpt !== '' ? max(1, (int) $shardsOpt) : null;
        $maxAttemptsOpt = $this->input->getOption('max-attempts');
        $maxAttempts = $maxAttemptsOpt !== null && $maxAttemptsOpt !== '' ? max(1, (int) $maxAttemptsOpt) : null;
        $noDlq = (bool) $this->input->getOption('no-dlq');

        $queueClass = $studly . 'Queue';
        $producerClass = $studly . 'Producer';
        $consumerClass = $studly . 'Consumer';
        $paths = $this->generatePaths();

        $files = [
            $paths['queue_path'] . '/' . $queueClass . '.php' => $this->queueStub(
                $queueClass,
                $producerClass,
                $consumerClass,
                $exchange,
                $label,
                $serialFields,
                $paths,
                $shards,
                $maxAttempts,
                $noDlq
            ),
            $paths['producer_path'] . '/' . $producerClass . '.php' => $this->producerStub($queueClass, $producerClass, $paths),
            $paths['consumer_path'] . '/' . $consumerClass . '.php' => $this->consumerStub($queueClass, $consumerClass, $label, $paths),
        ];

        foreach ($files as $path => $contents) {
            if (is_file($path) && ! $force) {
                $this->error('已存在（加 --force 覆盖）：' . $path);
                return;
            }
        }

        foreach ($files as $path => $contents) {
            $dir = dirname($path);
            if (! is_dir($dir) && ! mkdir($dir, 0755, true) && ! is_dir($dir)) {
                $this->error('无法创建目录：' . $dir);
                return;
            }
            file_put_contents($path, $contents);
            $this->line('created ' . $this->relative($path));
        }

        if (! $this->input->getOption('no-register')) {
            if ($this->register($exchange, $queueClass)) {
                $this->line('registered ' . $exchange . ' => ' . $queueClass . '::class');
            }
        }

        $this->info('完成。重启消费进程后会自动拉起该队列。');
        $this->line(sprintf('发送：\\%s\\%s::produce($data);', $paths['queue_namespace'], $queueClass));
        $this->line(sprintf('积压：php bin/hyperf.php amqp:info %s', $exchange));
        $this->line(sprintf('死信：php bin/hyperf.php amqp:dlq-info %s', $exchange));
    }

    /**
     * @return array{
     *   queue_namespace: string,
     *   producer_namespace: string,
     *   consumer_namespace: string,
     *   queue_path: string,
     *   producer_path: string,
     *   consumer_path: string,
     *   registry_file: string
     * }
     */
    private function generatePaths(): array
    {
        $config = ApplicationContext::getContainer()->get(ConfigInterface::class);
        $generate = (array) $config->get('amqp_sharded.generate', []);

        return [
            'queue_namespace' => (string) ($generate['queue_namespace'] ?? 'App\\Amqp\\Queue'),
            'producer_namespace' => (string) ($generate['producer_namespace'] ?? 'App\\Amqp\\Producer'),
            'consumer_namespace' => (string) ($generate['consumer_namespace'] ?? 'App\\Amqp\\Consumer'),
            'queue_path' => (string) ($generate['queue_path'] ?? BASE_PATH . '/app/Amqp/Queue'),
            'producer_path' => (string) ($generate['producer_path'] ?? BASE_PATH . '/app/Amqp/Producer'),
            'consumer_path' => (string) ($generate['consumer_path'] ?? BASE_PATH . '/app/Amqp/Consumer'),
            'registry_file' => (string) ($generate['registry_file'] ?? BASE_PATH . '/app/Amqp/Queue/AccountQueueRegistry.php'),
        ];
    }

    /**
     * @return array{0: string, 1: string} [StudlyName, exchange]
     */
    private function normalizeName(string $raw): array
    {
        $raw = trim($raw);
        $raw = (string) preg_replace('#(Queue|Producer|Consumer)$#i', '', $raw);
        $raw = str_replace(['-', '.', '/'], '_', $raw);
        $raw = (string) preg_replace('#[^A-Za-z0-9_]#', '', $raw);

        if ($raw === '') {
            return ['', ''];
        }

        if (str_contains($raw, '_')) {
            $parts = array_filter(explode('_', strtolower($raw)), static fn ($p) => $p !== '');
            $studly = implode('', array_map(static fn ($p) => ucfirst($p), $parts));
        } else {
            // accountRefund / AccountRefund → AccountRefund
            $studly = ucfirst($raw);
        }

        if ($studly === '') {
            return ['', ''];
        }

        return [$studly, lcfirst($studly)];
    }

    private function register(string $exchange, string $queueClass): bool
    {
        $registryPath = $this->generatePaths()['registry_file'];
        if (! is_file($registryPath)) {
            $this->error('注册表文件不存在：' . $registryPath);
            $this->line('请在 config/autoload/amqp_sharded.php 配置 generate.registry_file');
            return false;
        }

        $code = file_get_contents($registryPath);
        if ($code === false) {
            $this->error('无法读取注册表：' . $registryPath);
            return false;
        }

        if (str_contains($code, "'" . $exchange . "'")) {
            $this->warn('Registry 已包含 ' . $exchange . '，跳过注册');
            return false;
        }

        // 与 Queue 同命名空间，往 queues()/all() 返回数组加一行
        $entry = "            '{$exchange}' => {$queueClass}::class,\n";

        // 空注册表常为单行 return [];，需先展开再写入
        $updated = preg_replace(
            '#return\s*\[\s*\];#',
            "return [\n{$entry}        ];",
            $code,
            1,
            $count
        );

        if (! $count) {
            $updated = preg_replace(
                '#(return\s*\[\r?\n)#',
                '$1' . $entry,
                $code,
                1,
                $count
            );
        }

        if (! $count || $updated === null) {
            $this->error('无法自动写入 Registry，请手动添加：' . trim($entry));
            return false;
        }

        file_put_contents($registryPath, $updated);

        return true;
    }

    private function relative(string $path): string
    {
        return str_starts_with($path, BASE_PATH . '/')
            ? substr($path, strlen(BASE_PATH) + 1)
            : $path;
    }

    /**
     * @return list<string>
     */
    private function parseSerialFields(string $raw): array
    {
        $fields = [];
        foreach (explode(',', $raw) as $field) {
            $field = trim($field);
            if ($field === '') {
                continue;
            }
            $fields[] = $field;
        }

        return array_values(array_unique($fields));
    }

    /**
     * @param list<string> $serialFields
     * @param array<string, string> $paths
     */
    private function queueStub(
        string $queueClass,
        string $producerClass,
        string $consumerClass,
        string $exchange,
        string $label,
        array $serialFields,
        array $paths,
        ?int $shards = null,
        ?int $maxAttempts = null,
        bool $noDlq = false
    ): string {
        $serialLabel = implode(' + ', $serialFields);
        $extraMethods = '';
        $isDefault = $serialFields === ['tenant_id', 'busine_id'];
        if (! $isDefault) {
            $export = implode(",\n            ", array_map(
                static fn (string $f) => "'" . addslashes($f) . "'",
                $serialFields
            ));
            $extraMethods .= <<<PHP

    public static function serialFields(): array
    {
        return [
            {$export},
        ];
    }

PHP;
        }

        if ($shards !== null) {
            $extraMethods .= <<<PHP

    public static function shards(): int
    {
        return {$shards};
    }

PHP;
        }

        if ($maxAttempts !== null) {
            $extraMethods .= <<<PHP

    public static function maxAttempts(): int
    {
        return {$maxAttempts};
    }

PHP;
        }

        if ($noDlq) {
            $extraMethods .= <<<PHP

    public static function useDeadLetter(): bool
    {
        return false;
    }

PHP;
        }

        $queueNs = $paths['queue_namespace'];
        $consumerNs = $paths['consumer_namespace'];
        $producerNs = $paths['producer_namespace'];

        return <<<PHP
<?php

declare(strict_types=1);

namespace {$queueNs};

use {$consumerNs}\\{$consumerClass};
use {$producerNs}\\{$producerClass};
use Goletter\\Amqp\\Sharded\\TenantBusinessShardedQueue;

/** {$label}：同一 {$serialLabel} 串行，不同组合可并行。 */
final class {$queueClass} extends TenantBusinessShardedQueue
{
    public const EXCHANGE = '{$exchange}';
{$extraMethods}
    public static function consumerClass(): string
    {
        return {$consumerClass}::class;
    }

    public static function producerClass(): string
    {
        return {$producerClass}::class;
    }
}

PHP;
    }

    /**
     * @param array<string, string> $paths
     */
    private function producerStub(string $queueClass, string $producerClass, array $paths): string
    {
        $producerNs = $paths['producer_namespace'];
        $queueNs = $paths['queue_namespace'];

        return <<<PHP
<?php

declare(strict_types=1);

namespace {$producerNs};

use {$queueNs}\\{$queueClass};
use Goletter\\Amqp\\Sharded\\ShardedProducer;
use Hyperf\\Amqp\\Annotation\\Producer;

#[Producer(exchange: {$queueClass}::EXCHANGE, routingKey: '')]
class {$producerClass} extends ShardedProducer
{
    protected static function queueClass(): string
    {
        return {$queueClass}::class;
    }
}

PHP;
    }

    /**
     * @param array<string, string> $paths
     */
    private function consumerStub(string $queueClass, string $consumerClass, string $label, array $paths): string
    {
        $consumerNs = $paths['consumer_namespace'];
        $queueNs = $paths['queue_namespace'];

        return <<<PHP
<?php

declare(strict_types=1);

namespace {$consumerNs};

use {$queueNs}\\{$queueClass};
use Goletter\\Amqp\\Sharded\\ShardedConsumer;
use Hyperf\\Amqp\\Result;
use PhpAmqpLib\\Message\\AMQPMessage;

class {$consumerClass} extends ShardedConsumer
{
    protected function handle(mixed \$data, AMQPMessage \$message): Result
    {
        // TODO: 业务处理

        return Result::ACK;
    }

    protected static function queueClass(): string
    {
        return {$queueClass}::class;
    }
}

PHP;
    }
}
