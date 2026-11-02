<?php

declare(strict_types=1);

namespace Goletter\Amqp\Sharded;

use InvalidArgumentException;

/**
 * 可配置串行键的分片队列。
 *
 * 默认同一 tenant_id + busine_id 串行；子类用 serialFields() 改字段组合，例如：
 * - ['tenant_id', 'busine_id']（默认）
 * - ['tenant_id', 'account_id']
 * - ['account_id']
 *
 * 子类还需声明 EXCHANGE，并实现 consumerClass() / producerClass()。
 */
abstract class TenantBusinessShardedQueue extends ShardedQueue
{
    public const EXCHANGE = '';

    public static function exchange(): string
    {
        $exchange = static::EXCHANGE;
        if ($exchange === '') {
            throw new InvalidArgumentException(static::class . ' 必须定义 EXCHANGE');
        }

        return $exchange;
    }

    /**
     * 串行字段列表（顺序影响 serialKey）。
     * 同字段值组合进同一分片串行，不同组合可并行。
     *
     * @return list<string>
     */
    public static function serialFields(): array
    {
        return ['tenant_id', 'busine_id'];
    }

    /**
     * 字段别名：主字段名 => 可选用的备用键。
     * 默认 busine_id 可回落 business_id。
     *
     * @return array<string, list<string>>
     */
    public static function serialFieldAliases(): array
    {
        return [
            'busine_id' => ['business_id'],
        ];
    }

    public static function serialKey(array $data): string
    {
        $fields = static::serialFields();
        if ($fields === []) {
            throw new InvalidArgumentException(static::exchange() . ' serialFields() 不能为空');
        }

        $parts = [];
        $missing = [];
        foreach ($fields as $field) {
            $value = static::resolveSerialField($data, $field);
            if ($value === null || $value === '') {
                $missing[] = $field;
                continue;
            }
            $parts[] = (string) $value;
        }

        if ($missing !== []) {
            throw new InvalidArgumentException(sprintf(
                '%s 消息必须带串行字段：%s（缺失：%s）',
                static::exchange(),
                implode('+', $fields),
                implode(',', $missing)
            ));
        }

        return implode(':', $parts);
    }

    /**
     * 生产默认无额外间隔；串行仍由 prefetch=1 保证。
     * 若要对同组限速，子类可覆盖为 >0。
     */
    public static function intervalMs(): int
    {
        return 0;
    }

    protected static function resolveSerialField(array $data, string $field): mixed
    {
        if (array_key_exists($field, $data) && $data[$field] !== null && $data[$field] !== '') {
            return $data[$field];
        }

        $aliases = static::serialFieldAliases()[$field] ?? [];
        // 内置：busine_id ↔ business_id
        if ($field === 'busine_id' && ! in_array('business_id', $aliases, true)) {
            $aliases[] = 'business_id';
        }
        if ($field === 'business_id' && ! in_array('busine_id', $aliases, true)) {
            $aliases[] = 'busine_id';
        }

        foreach ($aliases as $alias) {
            if (array_key_exists($alias, $data) && $data[$alias] !== null && $data[$alias] !== '') {
                return $data[$alias];
            }
        }

        return null;
    }
}
