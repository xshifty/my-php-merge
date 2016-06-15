<?php
namespace Xshifty\MyPhpMerge\Actions;

use \Xshifty\MyPhpMerge\Merge\Rules\RuleContainer;
use \Xshifty\MyPhpMerge\Schema\MysqlConnection;
use \Xshifty\MyPhpMerge\Schema\Table\ForeignKey;
use \Xshifty\MyPhpMerge\Schema\Table\TableBase;

final class UpdateForeignKeys implements Action
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
        $foreignKeys = $this->table->getForeignKeys();
        foreach ($foreignKeys as $key) {
            $this->doUpdate($key);
        }
    }

    private function doUpdate(ForeignKey $key)
    {
        $sql = "UPDATE myphpmerge_{$key->getTable()} A, myphpmerge_{$key->getParentTable()} B
            SET    A.{$key->getName()} = B.myphpmerge__key__
            WHERE  (
                REPLACE(B.{$key->getParentColumn()}, ',', '|')
                REGEXP REPLACE(CONCAT('^', A.{$key->getName()}, '$'), ',', '$|^')
                OR
                REPLACE(A.{$key->getName()}, ',', '|')
                REGEXP REPLACE(CONCAT('^', B.{$key->getParentColumn()}, '$'), ',', '$|^')
                OR
                A.{$key->getName()} = B.{$key->getParentColumn()}
            )";

        $updated = $this->groupConnection->execute($sql);
        $status = '.';
        if (!$updated) {
            $status = '<error>!</error>';
        }
        cprint($status);
    }
}
