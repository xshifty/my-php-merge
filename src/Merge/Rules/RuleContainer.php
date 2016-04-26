<?php
namespace Xshifty\MyPhpMerge\Merge\Rules;

final class RuleContainer extends \SplMaxHeap
{
    const RULE_INTERFACE = "Xshifty\MyPhpMerge\Merge\Rules\Rule";
    const CREATE_INTERFACE = "Xshifty\MyPhpMerge\Merge\Rules\Create";
    const MERGE_INTERFACE = "Xshifty\MyPhpMerge\Merge\Rules\Merge";

    protected function compare($firstRule, $secondRule)
    {
        $firstPriority = 0;
        $secondPriority = 0;

        if (
            is_object($firstRule)
            && in_array(RuleContainer::RULE_INTERFACE, class_implements(get_class($firstRule)))
        ) {
            $firstPriority = $firstRule->getPriority();
        }

        if (
            is_object($secondRule)
            && in_array(RuleContainer::RULE_INTERFACE, class_implements(get_class($secondRule)))
        ) {
            $secondPriority = $secondRule->getPriority();
        }

        return $firstPriority - $secondPriority;
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
