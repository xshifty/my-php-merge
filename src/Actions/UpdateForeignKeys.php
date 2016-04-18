<?php
namespace Xshifty\MyPhpMerge\Actions;

use \Xshifty\MyPhpMerge\Merge\Rules\MergeRule;
use \Xshifty\MyPhpMerge\Schema\MysqlConnection;
use \Xshifty\MyPhpMerge\Schema\Table\TableBase;
use \Xshifty\MyPhpMerge\Merge\Rules\RuleContainer;
use \Xshifty\MyPhpMerge\Schema\Table\ForeignKey;

final class UpdateForeignKeys implements Action
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
        $foreignKeys = $this->table->getForeignKeys();
        foreach ($foreignKeys as $key) {
            $this->doUpdate($key);
        }
    }

    private function doUpdate(ForeignKey $key)
    {
        $this->groupConnection->execute(sprintf(
            'DROP TEMPORARY TABLE IF EXISTS temp_myphpmerge_%1$s',
            $key->getParentTable()
        ));

        $this->groupConnection->execute(sprintf(
            '
            CREATE TEMPORARY TABLE temp_myphpmerge_%1$s (
                SELECT * FROM myphpmerge_%1$s
                ORDER BY myphpmerge__key__
            )
            ',
            $key->getParentTable()
        ));

        $sql = sprintf(
            '
                UPDATE myphpmerge_%1$s A SET
                A.%2$s = (
                    SELECT B.myphpmerge__key__
                    FROM temp_myphpmerge_%3$s B
                    WHERE B.%4$s = A.%2$s
                    AND A.myphpmerge_schema = B.myphpmerge_schema
                    GROUP BY myphpmerge__key__
                    ORDER BY B.%4$s
                )
                WHERE A.myphpmerge_schema = %5$s
            ',
            $key->getTable(),
            $key->getName(),
            $key->getParentTable(),
            $key->getParentColumn(),
            $this->sourceConnection->quote($this->sourceConnection->getConfig()->schema)
        );

        $this->groupConnection->execute($sql);
        $this->groupConnection->execute(sprintf(
            'DROP TEMPORARY TABLE temp_myphpmerge_%1$s',
            $key->getParentTable()
        ));
    }
}
