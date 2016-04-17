<?php
namespace Xshifty\MyPhpMerge\Merge\Rules;

final class RuleContainer extends \SplMaxHeap
{
    protected function compare($firstRule, $secondRule)
    {
        $firstRule->priority = empty($firstRule->priority)
            ? 0 : intval($firstRule->priority);

        $secondRule->priority = empty($secondRule->priority)
            ? 0 : intval($secondRule->priority);

        return $firstRule->priority - $secondRule->priority;
    }

    public function getRule($tableName)
    {
        foreach ($this as $rule) {
            if ($rule->table == $tableName) {
                return $rule;
            }
        }
    }
}
