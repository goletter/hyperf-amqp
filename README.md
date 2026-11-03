# goletter/hyperf-amqp

Hyperf 分片 AMQP：同一串行键进同一分片串行，不同键可并行；支持重试、死信、耗时埋点与运维命令。

依赖：PHP >= 8.1、`hyperf/amqp` ~3.1（需先配好 RabbitMQ 连接）。

## 安装

```bash
composer require goletter/hyperf-amqp
php bin/hyperf.php vendor:publish goletter/hyperf-amqp
```

### 本地 path 开发（推荐，可自动加载 ConfigProvider）

Hyperf 通过 `Composer::getMergedExtra('hyperf')['config']` 收集各**已安装包**的 `extra.hyperf.config`。  
若只在根项目 `autoload.psr-4` 映射源码目录，包不会进入 `vendor/composer/installed.json`，**ConfigProvider / 命令不会自动注册**。

请用 path 仓库引入：

```json
{
  "require": {
    "goletter/hyperf-amqp": "1.1.0"
  },
  "repositories": [
    {
      "type": "path",
      "url": "packages/goletter/hyperf-amqp",
      "options": {
        "symlink": true,
        "versions": {
          "goletter/hyperf-amqp": "1.1.0"
        }
      }
    }
  ]
}
```

```bash
composer update goletter/hyperf-amqp -W
```

成功后会出现 `vendor/goletter/hyperf-amqp`（一般为软链），命令与注解扫描会自动合并。

> 不要同时保留根项目 `autoload.psr-4` 里的 `Goletter\\Amqp\\` 映射，以免与 path 包重复定义。

前置：项目已安装并配置 `config/autoload/amqp.php`（`hyperf/amqp` 连接池）。

## 接入步骤

### 1. 队列注册表

```php
namespace App\Amqp\Queue;

use Goletter\Amqp\Support\AbstractQueueRegistry;

final class AccountQueueRegistry extends AbstractQueueRegistry
{
    protected function queues(): array
    {
        return [
            'accountRecharge' => AccountRechargeQueue::class,
        ];
    }
}
```

`config/autoload/dependencies.php`：

```php
Goletter\Amqp\Contract\QueueRegistryInterface::class => App\Amqp\Queue\AccountQueueRegistry::class,
```

运维命令（`amqp:info` / `purge` / `dlq-*` 等）通过该注册表解析业务名。

### 2. 消费进程

```php
namespace App\Process;

use Goletter\Amqp\Contract\QueueRegistryInterface;
use Goletter\Amqp\Sharded\ShardedConsumeProcess;

class AccountShardedConsumeProcess extends ShardedConsumeProcess
{
    public string $name = 'account-sharded-consume';

    public int $nums = 1;

    protected function queues(): array
    {
        return $this->container->get(QueueRegistryInterface::class)->classes();
    }
}
```

在 `config/autoload/processes.php` 注册该类。

> **不要**再加 `#[Process]`，也不要给 Consumer 加 `#[Consumer]`：分片消费者由 `ShardedConsumeProcess` 按分片拉起，注解会导致双重消费。

### 3. 生成业务队列

```bash
php bin/hyperf.php amqp:make-queue accountRefund
php bin/hyperf.php amqp:make-queue accountRefund \
  --serial=tenant_id,code \
  --shards=32 \
  --max-attempts=1 \
  --no-dlq \
  --label=账户退款
```

| 选项 | 说明 |
|------|------|
| `--serial` | 串行字段，逗号分隔，默认 `tenant_id,busine_id` |
| `--shards` | 分片数（默认用基类 16） |
| `--max-attempts` | 含首次的最大执行次数（默认 3） |
| `--no-dlq` | 生成 `useDeadLetter(): false` |
| `--force` / `-f` | 已存在时覆盖 |
| `--no-register` | 不写入注册表 |
| `--label` | 中文说明（注释 / 日志） |

会生成 Queue / Producer / Consumer，并往 `amqp_sharded.generate.registry_file` 追加一行。

也可手写，分别继承：

- `Goletter\Amqp\Sharded\TenantBusinessShardedQueue`
- `Goletter\Amqp\Sharded\ShardedProducer`
- `Goletter\Amqp\Sharded\ShardedConsumer`

### 4. 发消息

```php
AccountRechargeQueue::produce([
    'tenant_id' => 1,
    'busine_id' => 2, // 也可用 business_id（默认别名）
    // ...
]);
```

`produce` 会写入 `produced_at`（毫秒），供消费侧算 `wait_ms` / `total_ms`。

推任意 `ProducerMessage`：

```php
produce(new SomeProducerMessage($payload));
```

### 5. Consumer 示例

```php
use Hyperf\Amqp\Result;
use PhpAmqpLib\Message\AMQPMessage;

class AccountRechargeConsumer extends ShardedConsumer
{
    protected function handle(mixed $data, AMQPMessage $message): Result
    {
        // 业务逻辑…
        // return Result::DROP; // 主动丢弃，不重试（有 DLX 则进死信）
        return Result::ACK;
    }

    protected static function queueClass(): string
    {
        return AccountRechargeQueue::class;
    }
}
```

行为：

- `handle` 返回 `ACK`：确认；若 `intervalMs() > 0`，ACK 前会等待（同片限速）
- `handle` **抛异常**：按 `attempts` / `maxAttempts` 延迟重投；超限 `DROP`
- `handle` 返回 `DROP`：不重试

### 6. 常用覆盖

| 方法 | 含义 | 默认 |
|------|------|------|
| `serialFields()` | 串行键字段 | `tenant_id,busine_id` |
| `serialFieldAliases()` | 字段别名 | `busine_id` ← `business_id` |
| `shards()` | 分片数 | `16` |
| `maxAttempts()` | 含首次的最大执行次数 | `3` |
| `useDeadLetter()` | 是否挂 DLX/DLQ | `true` |
| `intervalMs()` | ACK 前额外等待（同片限速） | `0` |
| `retryDelayMs()` | 重投前等待 | `2000` |

```php
public static function useDeadLetter(): bool
{
    return false; // 失败丢弃，不进死信
}
```

改 `shards()` / DLX 参数后若出现 `PRECONDITION_FAILED`，执行 `amqp:reset-shards`。

## 配置（仅代码生成）

发布后的 `config/autoload/amqp_sharded.php` **只影响** `amqp:make-queue` 生成路径与注册表文件，**不参与**运行时发消息 / 消费 / 死信。

```php
return [
    'generate' => [
        'queue_namespace' => 'App\\Amqp\\Queue',
        'producer_namespace' => 'App\\Amqp\\Producer',
        'consumer_namespace' => 'App\\Amqp\\Consumer',
        'queue_path' => BASE_PATH . '/app/Amqp/Queue',
        'producer_path' => BASE_PATH . '/app/Amqp/Producer',
        'consumer_path' => BASE_PATH . '/app/Amqp/Consumer',
        // 生成后往该文件的 queues() 追加 'name' => XxxQueue::class
        'registry_file' => BASE_PATH . '/app/Amqp/Queue/AccountQueueRegistry.php',
    ],
];
```

项目目录结构变了只改这里即可。

## 日志与耗时

消费成功 / 失败会写 category（logger name）`queue`：

| 字段 | 含义 |
|------|------|
| `wait_ms` | 入队到开始消费 |
| `handle_ms` | `handle()` 耗时 |
| `total_ms` | 入队到结束 |
| `attempts` | 当前重试次数 |
| `shard` / `key` | 分片与串行键 |

`AmqpLogger` 优先调用 `Goletter\Utils\logging()`（需 `goletter/hyperf-utils`），否则回落 `hyperf/logger` 的 `LoggerFactory`。二者都没有时静默跳过。

建议：

```bash
composer require goletter/hyperf-utils
# 或
composer require hyperf/logger
```

## 运维命令

```bash
# 积压汇总（可省略 name 看全部）
php bin/hyperf.php amqp:info
php bin/hyperf.php amqp:info accountInsight --verbose-shards

# 死信积压
php bin/hyperf.php amqp:dlq-info accountRecharge

# 清空死信（需 --force）
php bin/hyperf.php amqp:dlq-purge accountRecharge --force

# 死信重投（重置 attempts；需该业务 useDeadLetter=true）
php bin/hyperf.php amqp:dlq-replay accountRecharge --limit=10
php bin/hyperf.php amqp:dlq-replay accountRecharge --dry-run
php bin/hyperf.php amqp:dlq-replay accountRecharge --where=tenant_id:1 --where=account_id:5 --scan=200

# 清空业务分片消息，保留队列定义（需 --force）
php bin/hyperf.php amqp:purge accountInsight --force
php bin/hyperf.php amqp:purge accountInsight --force --dlq

# 删除分片队列以便按新参数重建（需 --force；默认保留死信）
php bin/hyperf.php amqp:reset-shards accountRecharge --force
php bin/hyperf.php amqp:reset-shards accountRecharge --force --purge-dlq
```

### `amqp:dlq-replay` 选项

| 选项 | 说明 |
|------|------|
| `--limit` | 最多重投条数（有 `--where` 时指匹配条数），默认 10 |
| `--scan` | 有 `--where` 时最多扫描死信条数，默认 500 |
| `--where=field:value` | 按 payload 过滤，可多次（AND） |
| `--dry-run` | 只打印，不投递 / ACK |

## 设计要点

- 同一 `serialKey` → 同一分片；片内 `prefetch=1` + `x-single-active-consumer` 串行
- 队列类型为 **quorum**
- 不同 key 尽量打散到不同分片并行
- 失败：`attempts` 计数重投；超过 `maxAttempts` → `DROP`（有 DLX 则进死信）
- `amqp:dlq-replay` 重投时会重置 `attempts`
