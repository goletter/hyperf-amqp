# goletter/hyperf-amqp

Hyperf 分片 AMQP：同一串行键进同一分片串行，不同键可并行；支持重试、死信开关与运维命令。

## 安装

```bash
composer require goletter/hyperf-amqp
php bin/hyperf.php vendor:publish goletter/hyperf-amqp
```

本地 path 开发可在项目 `composer.json` autoload：

```json
"Goletter\\Amqp\\": "packages/goletter/hyperf-amqp/src/"
```

并确保注解扫描包含包目录，或注册 `ConfigProvider`。

## 用法

1. 实现 `Goletter\Amqp\Contract\QueueRegistryInterface`（应用侧注册表）
2. 在 `dependencies.php` 绑定该接口
3. Queue 继承 `TenantBusinessShardedQueue`，Producer/Consumer 继承对应基类
4. 消费进程继承 `ShardedConsumeProcess`，`queues()` 返回注册表 `classes()`

```php
AccountRechargeQueue::produce($data);

public static function useDeadLetter(): bool
{
    return false; // 失败丢弃，不进死信
}
```

## 运维

```bash
php bin/hyperf.php amqp:make-queue accountRefund
php bin/hyperf.php amqp:dlq-info accountRecharge
php bin/hyperf.php amqp:dlq-replay accountRecharge --limit=10
php bin/hyperf.php amqp:reset-shards accountRecharge --force
```
