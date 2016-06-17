<?php
namespace Xshifty\MyPhpMerge\Actions;

use Xshifty\MyPhpMerge\Merge\Rules\Rule;
use Xshifty\MyPhpMerge\Schema\MysqlConnection;

final class PrepareTable implements Action
{
    private $mergeRule;
    private $templateConnection;
    private $groupConnection;

    public function __construct(
        Rule $mergeRule,
        MysqlConnection $templateConnection,
        MysqlConnection $groupConnection
    ) {
        $this->mergeRule = $mergeRule;
        $this->templateConnection = $templateConnection;
        $this->groupConnection = $groupConnection;
    }

    public function execute()
    {
        echo '.';
        $this->groupConnection->execute(sprintf(
            'DROP TABLE IF EXISTS `%s`',
            $this->mergeRule->table
        ));

        $this->groupConnection->execute(sprintf(
            'DROP TABLE IF EXISTS `myphpmerge_%s`',
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

        $createSql = sprintf(
            'CREATE TABLE `myphpmerge_%1$s` (SELECT * FROM `%1$s`)',
            $this->mergeRule->table
        );
        $this->groupConnection->execute($createSql);

        if (!$this->groupConnection->hasTable('myphpmerge_' . $this->mergeRule->table)) {
            throw new \RuntimeException("Can't be possible create transitional table for table {$rule->table}");
        }

        $foreignKeysQuery = '';
        array_walk($this->mergeRule->foreignKeys, function ($row) use (&$foreignKeysQuery) {
            $foreignKeysQuery .= "CHANGE `{$row['key']}` `{$row['key']}` VARCHAR(255) NULL DEFAULT NULL," . PHP_EOL;
        }, $this->mergeRule->foreignKeys);

        $alterSql = sprintf('
            ALTER TABLE `myphpmerge_%1$s`
                ADD COLUMN `myphpmerge_schema` VARCHAR(50) FIRST,
                ADD COLUMN `myphpmerge_grouped_keys` VARCHAR(1000) FIRST,
                ADD COLUMN `myphpmerge__key__` VARCHAR(1000) FIRST,
                ADD COLUMN `myphpmerge_%2$s` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT FIRST,
                CHANGE `%2$s` `%2$s` VARCHAR(255) NULL DEFAULT NULL,
                %3$s
                ADD PRIMARY KEY (`myphpmerge_%2$s`)
            ',
            $this->mergeRule->table,
            $this->mergeRule->primaryKey,
            $foreignKeysQuery
        );

        $this->groupConnection->execute($alterSql);
    }
}
