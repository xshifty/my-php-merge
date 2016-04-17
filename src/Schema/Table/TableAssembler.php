<?php
namespace Xshifty\MyPhpMerge\Schema\Table;

use \Xshifty\MyPhpMerge\Schema\MysqlConnection;
use \Xshifty\MyPhpMerge\Merge\Rules\Rule;

final class TableAssembler
{
    private $templateConnection;

    public function __construct(MysqlConnection $templateConnection)
    {
        $this->templateConnection = $templateConnection;
    }

    public function assembly(Rule $mergeRule)
    {
        $columns = array_map(function ($row) {
            return $row['Field'];
        }, $mergeRule->getTableColumns());
        $table = new Table($mergeRule->table);

        foreach ($columns as $column) {
            $this->bindColumn($mergeRule, $table, $column);
        }

        return $table;
    }

    private function bindColumn(Rule $mergeRule, TableBase $table, $column)
    {
        if (!empty($mergeRule->primaryKey) && $mergeRule->primaryKey == $column) {
            return $table->addColumn(new PrimaryKey($table, $column));
        }

        $foreignKey = array_reduce($mergeRule->foreignKeys, function ($initial, $current) use ($column) {
            if ($current['key'] == $column) {
                $initial = $current;
            }

            return $initial;
        }, null);

        if (!empty($foreignKey)) {
            $columnForeignKey = new ForeignKey($table, $column);
            $columnForeignKey->setParentTable($foreignKey['reference']);
            $columnForeignKey->setParentColumn($foreignKey['foreign_key']);

            return $table->addColumn($columnForeignKey);
        }
    }
}
