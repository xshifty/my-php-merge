<?php
namespace Xshifty\MyPhpMerge\Actions;

use Xshifty\MyPhpMerge\Merge\Rules\MergeRule;
use Xshifty\MyPhpMerge\Schema\MysqlConnection;

final class MoveMergeData implements Action
{
    private $mergeRule;
    private $sourceConnection;
    private $groupConnection;

    public function __construct(
        MergeRule $mergeRule,
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

        $accumColumnsName = array_map(function ($row) use ($columnsPrimaryKey, $accumPrimaryKey) {
            if (in_array($row, ['myphpmerge_schema', $accumPrimaryKey['Field']])) {
                return null;
            }

            return 'myphpmerge__key__' == $row
                ? "myphpmerge__key__ AS '{$columnsPrimaryKey['Field']}'" : $row;
        }, $accumColumnsName);
        $accumColumnsName = array_filter($accumColumnsName);

        $sql = sprintf(
            'REPLACE INTO %1$s (%2$s) (SELECT %3$s FROM myphpmerge_%1$s %4$s ORDER BY myphpmerge_%5$s)',
            $this->mergeRule->table,
            implode(', ', $columnsName),
            implode(', ', $accumColumnsName),
            !empty($this->mergeRule->unique) && count($this->mergeRule->unique)
                ? 'GROUP BY ' . implode(', ', $this->mergeRule->unique) : '',
            $columnsPrimaryKey['Field']
        );

        $this->groupConnection->execute($sql);
    }
}
