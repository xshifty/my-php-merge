<?php
namespace Xshifty\MyPhpMerge\Actions;

use \Xshifty\MyPhpMerge\Merge\Rules\RuleContainer;
use \Xshifty\MyPhpMerge\Schema\MysqlConnection;
use \Xshifty\MyPhpMerge\Schema\Table\TableBase;

final class UpdatePrimaryKeys implements Action
{
    private $table;
    private $groupConnection;
    private $ruleContainer;

    public function __construct(
        TableBase $table,
        MysqlConnection $groupConnection,
        RuleContainer $ruleContainer
    ) {
        $this->table = $table;
        $this->groupConnection = $groupConnection;
        $this->ruleContainer = $ruleContainer;
    }

    public function execute()
    {
        $where = '';
        $unique = $this->ruleContainer->getRule($this->table->getName())->unique;

        if (!count($unique)) {
            return true;
        }

        $this->groupConnection->execute(sprintf(
            'CREATE TABLE IF NOT EXISTS pkey_myphpmerge_%1$s (SELECT * FROM myphpmerge_%1$s)',
            $this->table->getName()
        ));

        $where = [];
        foreach ($unique as $column) {
            $where[] = "IF(ISNULL(A.{$column}), ISNULL(B.{$column}), B.{$column} = A.{$column})";
        }
        $where = 'WHERE ' . implode(PHP_EOL . ' AND ', $where);

        $group = 'GROUP BY B.' . implode(
            ', B.',
            $this->ruleContainer->getRule($this->table->getName())->unique
        );

        $pkey = $this->table->getPrimaryKey();
        $keyReplace = $pkey->getType() != 'INTEGER'
        ? 'myphpmerge__key__' : 'myphpmerge_' . $pkey->getName();

        $sql = sprintf(
            '
                UPDATE myphpmerge_%1$s A
                SET A.myphpmerge__key__ = (
                    SELECT %5$s
                    FROM pkey_myphpmerge_%1$s B
                    %3$s
                    %4$s
                    ORDER BY B.myphpmerge_%2$s
                    LIMIT 1
                )
            ',
            $this->table->getName(),
            $this->table->getPrimaryKey()->getName(),
            $where,
            $group,
            $keyReplace
        );

        $updated = $this->groupConnection->execute($sql);
        $status = '.';
        if (!$updated) {
            $status = '<error>!</error>';
        }
        cprint($status);

        $this->groupConnection->execute(sprintf(
            'DROP TABLE IF EXISTS pkey_myphpmerge_%1$s',
            $this->table->getName()
        ));
    }
}
