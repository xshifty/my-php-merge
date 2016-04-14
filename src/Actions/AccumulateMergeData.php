<?php
namespace Xshifty\MyPhpMerge\Actions;

use Xshifty\MyPhpMerge\Merge\Rules\MergeRule;
use Xshifty\MyPhpMerge\Schema\MysqlConnection;

final class AccumulateMergeData implements Action
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
        $columnsDescription = $this->mergeRule->getTableColumns();
        $columnsName = array_map(function ($row) {
            return $row['Field'];
        }, $columnsDescription);

        $querySql = sprintf(
            'SELECT %1$s FROM %2$s',
            join(', ', $columnsName),
            $this->mergeRule->table
        );

        $data = $this->sourceConnection->query($querySql);
        if (empty($data)) {
            return true;
        }

        foreach ($data as $row) {
            $values = array_map(\Closure::bind(function ($value) {
                if (is_null($value)) {
                    return 'NULL';
                }
                return $this->groupConnection->quote($value);
            }, $this, $this), $row);

            $sql = sprintf(
                'INSERT INTO myphpmerge_%1$s (%2$s) VALUES (%3$s)',
                $this->mergeRule->table,
                join(", ", array_keys($row)),
                join(', ', $values)
            );

            $this->groupConnection->execute($sql);
        }
    }
}
