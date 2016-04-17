<?php
namespace Xshifty\MyPhpMerge\Actions;

use \Xshifty\MyPhpMerge\Merge\Rules\MergeRule;
use \Xshifty\MyPhpMerge\Schema\MysqlConnection;
use \Xshifty\MyPhpMerge\Schema\Table\TableBase;
use \Xshifty\MyPhpMerge\Merge\Rules\RuleContainer;

final class UpdateKeys implements Action
{
    private $table;
    private $sourceConnection;
    private $groupConnection;
    private $ruleContainer;

    public function __construct(
        TableBase $table,
        MysqlConnection $sourceConnection,
        MysqlConnection $groupConnection,
        RuleContainer $ruleContainer
    )
    {
        $this->table = $table;
        $this->sourceConnection = $sourceConnection;
        $this->groupConnection = $groupConnection;
        $this->ruleContainer = $ruleContainer;
    }

    public function execute()
    {
        echo '.';
        $rule = $this->ruleContainer->getRule(
            $this->table->getName()
        );

        $this->updatePrimaryKeys();
    }

    public function updatePrimaryKeys()
    {
        $where = '';
        $unique = $this->ruleContainer->getRule($this->table->getName())->unique;

        if (!count($unique)) {
            return true;
        }

        $where = [];
        foreach ($unique as $column) {
            $where[] = "{$column} = A.{$column}";
        }
        $where = 'WHERE ' . implode(' AND ', $where);

        $this->groupConnection->execute(sprintf(
            'CREATE TEMPORARY TABLE temp_myphpmerge_%1$s (SELECT * FROM myphpmerge_%1$s)',
            $this->table->getName()
        ));

        $this->groupConnection->execute(sprintf(
            '
                UPDATE myphpmerge_%1$s A
                SET A.myphpmerge__key__ = (
                    SELECT myphpmerge_%2$s FROM temp_myphpmerge_%1$s
                    %3$s
                    ORDER BY myphpmerge_%2$s
                    LIMIT 1
                )
            ',
            $this->table->getName(),
            $this->table->getPrimaryKey()->getName(),
            $where
        ));

        $this->groupConnection->execute(sprintf(
            'DROP TEMPORARY TABLE temp_myphpmerge_%1$s',
            $this->table->getName()
        ));
    }
}
