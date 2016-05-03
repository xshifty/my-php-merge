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
    private $groupConnection;
    private $ruleContainer;

    public function __construct(
        TableBase $table,
        MysqlConnection $groupConnection,
        RuleContainer $ruleContainer
    )
    {
        $this->table = $table;
        $this->groupConnection = $groupConnection;
        $this->ruleContainer = $ruleContainer;
    }

    public function execute()
    {
        $foreignKeys = $this->table->getForeignKeys();
        foreach ($foreignKeys as $key) {
            $this->doUpdate($key);
        }
    }

    private function doUpdate(ForeignKey $key)
    {
        $this->groupConnection->execute(sprintf(
            'DROP TABLE IF EXISTS fkey_myphpmerge_%1$s',
            $key->getParentTable()
        ));

        $this->groupConnection->execute(sprintf(
            '
            CREATE TABLE fkey_myphpmerge_%1$s (
                SELECT * FROM myphpmerge_%1$s
            )
            ',
            $key->getParentTable()
        ));

        $sql = sprintf(
            '
                UPDATE  myphpmerge_%1$s A, fkey_myphpmerge_%2$s B
                SET     A.%3$s = B.myphpmerge__key__
                WHERE   B.%4$s = A.%3$s
                AND     A.myphpmerge_schema = B.myphpmerge_schema
            ',

            $key->getTable(),
            $key->getParentTable(),
            $key->getName(),
            $key->getParentColumn()
        );

        $updated = $this->groupConnection->execute($sql);
        $status = '.';
        if (!$updated) {
            $status = '<error>!</error>';
        }
        cprint($status);

        $this->groupConnection->execute(sprintf(
            'DROP TABLE fkey_myphpmerge_%1$s',
            $key->getParentTable()
        ));
    }
}
