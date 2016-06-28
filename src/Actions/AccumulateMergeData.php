<?php
namespace Xshifty\MyPhpMerge\Actions;

use Xshifty\MyPhpMerge\Merge\Rules\Rule;
use Xshifty\MyPhpMerge\Schema\MysqlConnection;

final class AccumulateMergeData implements Action
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

        $querySql = sprintf(
            'SELECT %1$s FROM `%2$s`',
            join(', ', $columnsName),
            $this->mergeRule->table
        );

        $data = $this->sourceConnection->query($querySql);
        if (empty($data)) {
            return true;
        }

        $autoIncrement = isset($this->mergeRule->autoIncrement)
        ? $this->mergeRule->autoIncrement
        : true
        ;

        foreach ($data as $row) {
            $values = array_map(\Closure::bind(function ($value) {
                if (is_null($value)) {
                    return 'NULL';
                }
                return $this->groupConnection->quote($value);
            }, $this), $row);

            $insertTemplate = '
                INSERT INTO `myphpmerge_%1$s` (
                    `myphpmerge_schema`,
                    `myphpmerge__key__`,
                    %2$s
                ) VALUES (
                    %4$s,
                    %5$s,
                    %3$s
                )
            ';

            if (!$autoIncrement) {
                $insertTemplate = '
                    INSERT INTO `myphpmerge_%1$s` (
                        `myphpmerge_schema`,
                        `myphpmerge_' . $this->mergeRule->primaryKey . '`,
                        `myphpmerge__key__`,
                        %2$s
                    ) VALUES (
                        %4$s,
                        %5$s,
                        %5$s,
                        %3$s
                    )
                ';
            }

            $insert = sprintf($insertTemplate,
                $this->mergeRule->table,
                join(", ", array_keys($row)),
                join(', ', $values),
                "'{$this->sourceConnection->getConfig()->schema}'",
                $values[$this->mergeRule->primaryKey]
            );

            $this->groupConnection->execute($insert);

            $primaryKeyPrefix = count($this->mergeRule->unique) < 1 ? 'myphpmerge_' : '';

            $this->groupConnection->execute(sprintf(
                '
                    UPDATE `myphpmerge_%1$s` A
                    SET A.`myphpmerge__key__` = A.%2$s
                ',
                $this->mergeRule->table,
                $primaryKeyPrefix . $this->mergeRule->primaryKey
            ));
        }
    }
}
