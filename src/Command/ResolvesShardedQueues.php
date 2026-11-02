<?php

declare(strict_types=1);

namespace Goletter\Amqp\Command;

use Goletter\Amqp\Contract\QueueRegistryInterface;
use Goletter\Amqp\Sharded\ShardedQueue;
use Hyperf\Context\ApplicationContext;

/**
 * 解析注册表业务名，统一未知业务 / 缺参报错文案。
 */
trait ResolvesShardedQueues
{
    protected function queueRegistry(): QueueRegistryInterface
    {
        return ApplicationContext::getContainer()->get(QueueRegistryInterface::class);
    }

    /**
     * @return array<string, class-string<ShardedQueue>>
     */
    protected function resolveShardedQueues(?string $name, bool $allowAllWhenEmpty = false): array
    {
        $registry = $this->queueRegistry();
        $all = $registry->all();
        $wantAll = $this->input->hasOption('all') && (bool) $this->input->getOption('all');

        if ($wantAll) {
            return $all;
        }

        if ($name === null || $name === '') {
            if ($allowAllWhenEmpty) {
                return $all;
            }

            $this->error('请指定业务名，或加 --all 处理全部业务。');
            $this->line('可选业务：' . $this->availableQueueNames());
            $this->line('示例：' . $this->exampleCommandLine());

            return [];
        }

        $queueClass = $registry->get($name);
        if ($queueClass === null) {
            $this->error(sprintf('未知业务「%s」。', $name));
            $this->line('可选业务：' . $this->availableQueueNames());
            $this->line('提示：业务名需与队列注册表中的 key 一致（如 accountRecharge）。');

            return [];
        }

        return [$name => $queueClass];
    }

    protected function availableQueueNames(): string
    {
        $names = array_keys($this->queueRegistry()->all());

        return $names === [] ? '(注册表为空)' : implode(', ', $names);
    }

    protected function exampleCommandLine(): string
    {
        return 'php bin/hyperf.php ' . $this->getName() . ' accountRecharge';
    }

    protected function requireForce(string $dangerHint): bool
    {
        if ($this->input->getOption('force')) {
            return true;
        }

        $this->error('已拒绝执行：缺少 --force。');
        $this->line('原因：' . $dangerHint);
        $this->line('确认无误后请加上 --force，例如：');
        $this->line('  ' . $this->exampleCommandLine() . ' --force');

        return false;
    }
}
