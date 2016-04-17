<?php
namespace Xshifty\MyPhpMerge\Actions;

use Xshifty\MyPhpMerge\Merge\Rules\MergeRule;
use Xshifty\MyPhpMerge\Schema\MysqlConnection;

final class PrepareTable implements Action
{
    private $mergeRule;
    private $templateConnection;
    private $groupConnection;

    public function __construct(
        MergeRule $mergeRule,
        MysqlConnection $templateConnection,
        MysqlConnection $groupConnection
    )
    {
        $this->mergeRule = $mergeRule;
        $this->templateConnection = $templateConnection;
        $this->groupConnection = $groupConnection;
    }

    public function execute()
    {
        echo '.';
        $this->groupConnection->execute(sprintf(
            'DROP TABLE IF EXISTS %s',
            $this->mergeRule->table
        ));

        $this->groupConnection->execute(sprintf(
            'DROP TABLE IF EXISTS myphpmerge_%s',
            $this->mergeRule->table
        ));

        $copySucceed = $this->groupConnection->copyTable(
            $this->mergeRule->table,
            $this->templateConnection
        );

        if (!$copySucceed) {
            throw new \RuntimeException(sprintf(
                "Can't copy %s table %s to %s",
                $this->templateConnection->getConfig()->schema,
                $this->mergeRule->table,
                $this->groupConnection->getConfig()->schema
            ));
        }

        $this->groupConnection->execute(sprintf(
            'CREATE TABLE myphpmerge_%1$s (SELECT * FROM %1$s)',
            $this->mergeRule->table
        ));

        if (!$this->groupConnection->hasTable('myphpmerge_' . $this->mergeRule->table)) {
            throw new \RuntimeException("Can't be possible create transitional table for table {$rule->table}");
        }

        $alterSql = sprintf('
            ALTER TABLE myphpmerge_%1$s
                ADD COLUMN myphpmerge_schema VARCHAR(50) FIRST,
                ADD COLUMN myphpmerge__key__ BIGINT UNSIGNED FIRST,
                ADD COLUMN myphpmerge_%2$s BIGINT UNSIGNED NOT NULL AUTO_INCREMENT FIRST,
                ADD PRIMARY KEY (myphpmerge_%2$s)
            ',
            $this->mergeRule->table,
            $this->mergeRule->primaryKey
        );

        $this->groupConnection->execute($alterSql);
    }
}
