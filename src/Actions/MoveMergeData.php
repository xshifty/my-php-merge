<?php
namespace Xshifty\MyPhpMerge\Actions;

use Xshifty\MyPhpMerge\Merge\Rules\Rule;
use Xshifty\MyPhpMerge\Schema\MysqlConnection;

final class MoveMergeData implements Action
{
    private $mergeRule;
    private $sourceConnection;
    private $groupConnection;

    public function __construct(
        Rule $mergeRule,
        MysqlConnection $sourceConnection,
        MysqlConnection $groupConnection
    )
    {
        $this->mergeRule = $mergeRule;
        $this->sourceConnection = $sourceConnection;
        $this->groupConnection = $groupConnection;
    }

    public function execute()
    {
        echo '.';

        $columnsDescription = $this->mergeRule->getTableColumns();
        $columnsName = array_map(function ($row) {
            return $row['Field'];
        }, $columnsDescription);
        $columnsPrimaryKey = array_reduce($columnsDescription, function ($initial, $current) {
            if (!empty($initial['Key']) && $initial['Key'] == 'PRI') {
                return $initial;
            }

            if (!empty($current['Key']) && $current['Key'] == 'PRI') {
                $initial = $current;
                return $initial;
            }
        });

        $accumColumnsDescription = $this->groupConnection->query("DESCRIBE myphpmerge_{$this->mergeRule->table}");
        $accumPrimaryKey = array_reduce($accumColumnsDescription, function ($initial, $current) {
            if (!empty($initial['Key']) && $initial['Key'] == 'PRI') {
                return $initial;
            }

            if (!empty($current['Key']) && $current['Key'] == 'PRI') {
                $initial = $current;
                return $initial;
            }
        });
        $accumColumnsName = array_map(function ($row) {
            return $row['Field'];
        }, $accumColumnsDescription);

        $accumColumnsName = array_filter($accumColumnsName, function ($row) use ($columnsPrimaryKey) {
            return $columnsPrimaryKey['Field'] == $row ? false : $row;
        });

        $unique = !empty($this->mergeRule->unique) ? $this->mergeRule->unique : [];
        $accumColumnsName = array_map(function ($row) use ($columnsPrimaryKey, $accumPrimaryKey, $unique) {
            if (in_array($row, ['myphpmerge_schema', $accumPrimaryKey['Field']])) {
                return null;
            }

            $maxRow = $row;
            if (count($unique)) {
                $maxRow = "MAX({$row}) AS '{$row}'";
            }

            return 'myphpmerge__key__' == $row
                ? "myphpmerge__key__ AS '{$columnsPrimaryKey['Field']}'" : $maxRow;
        }, $accumColumnsName);
        $accumColumnsName = array_filter($accumColumnsName);

        $sql = sprintf(
            '
                REPLACE INTO `%1$s` (%2$s) (
                    SELECT      %3$s
                    FROM        `myphpmerge_%1$s`
                    %4$s
                    ORDER BY    LPAD(`myphpmerge__key__`, 10, "0") ASC
                )
            ',
            $this->mergeRule->table,
            implode(', ', $columnsName),
            implode(', ', $accumColumnsName),
            !empty($this->mergeRule->unique) && count($this->mergeRule->unique)
                ? 'GROUP BY ' . implode(', ', $this->mergeRule->unique) : ''
        );

        $this->groupConnection->execute($sql);
    }
}
