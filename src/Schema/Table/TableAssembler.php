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
        $columns = $mergeRule->getTableColumns();
        $table = new Table($mergeRule->table);

        foreach ($columns as $column) {
            $this->bindColumn($mergeRule, $table, $column['Field'], $column['Type']);
        }

        return $table;
    }

    private function bindColumn(Rule $mergeRule, TableBase $table, $columnName, $columnType)
    {
        if (!empty($mergeRule->primaryKey) && $mergeRule->primaryKey == $columnName) {
            return $table->addColumn(new PrimaryKey($table, $columnName, $columnType));
        }

        $foreignKey = array_reduce($mergeRule->foreignKeys, function ($initial, $current) use ($columnName) {
            if ($current['key'] == $columnName) {
                $initial = $current;
            }

            return $initial;
        }, null);

        if (!empty($foreignKey)) {
            $columnForeignKey = new ForeignKey($table->getName(), $columnName, $columnType);
            $columnForeignKey->setParentTable($foreignKey['reference']);
            $columnForeignKey->setParentColumn($foreignKey['foreign_key']);

            return $table->addColumn($columnForeignKey);
        }

        $table->addColumn(new Column($table->getName(), $columnName, $columnType));
    }
}
