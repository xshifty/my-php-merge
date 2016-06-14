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
    ) {
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

        $accumColumnsDescription = $this->groupConnection->query("DESCRIBE myphpmerge_{$this->mergeRule->table}");

        $accumColumnsName = array_map(function ($row) use ($columnsName) {

            if ($row['Field'] == 'myphpmerge__key__') {
                return 'myphpmerge__key__ AS id';
            }

            $inOriginalTable = array_search($row['Field'], $columnsName);
            return $inOriginalTable ? $row['Field'] : false;
        }, $accumColumnsDescription);

        $accumColumnsName = array_filter($accumColumnsName);

        $sql = sprintf(
            '
                REPLACE INTO `%1$s` (%2$s) (
                    SELECT      %3$s
                    FROM        `myphpmerge_%1$s`
                    ORDER BY    LPAD(`myphpmerge__key__`, 10, "0") ASC
                )
            ',
            $this->mergeRule->table,
            implode(', ', $columnsName),
            implode(', ', $accumColumnsName)
        );

        $this->groupConnection->execute($sql);
    }
}
