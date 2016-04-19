<?php
namespace Xshifty\MyPhpMerge\Actions;

use \Xshifty\MyPhpMerge\Merge\Rules\MergeRule;
use \Xshifty\MyPhpMerge\Schema\MysqlConnection;
use \Xshifty\MyPhpMerge\Schema\Table\TableBase;
use \Xshifty\MyPhpMerge\Merge\Rules\RuleContainer;

final class UpdatePrimaryKeys implements Action
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

        $where = '';
        $unique = $this->ruleContainer->getRule($this->table->getName())->unique;

        if (!count($unique)) {
            return true;
        }

        $where = [];
        foreach ($unique as $column) {
            $where[] = "IF(ISNULL(A.{$column}), ISNULL(B.{$column}), B.{$column} = A.{$column})";
        }
        $where = 'WHERE ' . implode(PHP_EOL . ' AND ', $where);

        $this->groupConnection->execute(sprintf(
            'CREATE TABLE IF NOT EXISTS temp_myphpmerge_%1$s (SELECT * FROM myphpmerge_%1$s)',
            $this->table->getName()
        ));

        $sql = sprintf(
            '
                UPDATE myphpmerge_%1$s A
                SET A.myphpmerge__key__ = (
                    SELECT myphpmerge__key__
                    FROM temp_myphpmerge_%1$s B
                    %3$s
                    GROUP BY B.%4$s
                    ORDER BY B.myphpmerge_%2$s
                    LIMIT 1
                )
            ',
            $this->table->getName(),
            $this->table->getPrimaryKey()->getName(),
            $where,
            implode(', B.', $this->ruleContainer->getRule($this->table->getName())->unique)
        );

        $this->groupConnection->execute(sprintf('DROP TABLE IF EXISTS temp_myphpmerge_%1$s', $this->table->getName()));
        $this->groupConnection->execute($sql);
    }
}
