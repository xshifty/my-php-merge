<?php
namespace Xshifty\MyPhpMerge\Actions;

use Xshifty\MyPhpMerge\Merge\Rules\Rule;
use Xshifty\MyPhpMerge\Schema\MysqlConnection;

final class FlatDuplicateDataSecondFactor implements Action
{
    private $mergeRule;
    private $groupConnection;

    public function __construct(
        Rule $mergeRule,
        MysqlConnection $groupConnection
    ) {
        $this->mergeRule = $mergeRule;
        $this->groupConnection = $groupConnection;
    }

    public function execute()
    {
        echo '.';

        $this->groupConnection->execute(sprintf(
            'DROP TABLE IF EXISTS `myphpmerge_%s_flat`',
            $this->mergeRule->table
        ));

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

        $accumColumnsNameOriginal = $accumColumnsName;

        $unique = !empty($this->mergeRule->unique) ? $this->mergeRule->unique : [];
        $foreignKeys = $this->mergeRule->foreignKeys;
        $table = $this->mergeRule->table;
        $originalAccumColumnsName = $accumColumnsName;
        $accumColumnsName = array_map(function ($row) use ($accumPrimaryKey, $unique, $foreignKeys, $table) {

            $maxRow = $row;
            $isUnique = count($unique);
            $isForeignKey = array_search($row, array_column($foreignKeys, 'key'));

            if ($isUnique) {
                $maxRow = "MAX({$row}) AS '{$row}'";
            }

            if ('myphpmerge_grouped_keys' == $row && $isUnique) {
                return "GROUP_CONCAT(DISTINCT myphpmerge__key__) AS myphpmerge_grouped_keys";
            }

            if ('id' == $row && $isUnique) {
                return "GROUP_CONCAT(DISTINCT id) AS id";
            }

            if ('myphpmerge_schema' == $row && $isUnique) {
                return "GROUP_CONCAT(DISTINCT myphpmerge_schema) AS myphpmerge_schema";
            }

            if ('myphpmerge__key__' == $row && $isUnique) {
                return "MIN(myphpmerge__key__) AS myphpmerge__key__";
            }

            if ($isForeignKey && $isUnique) {
                $this->groupConnection->execute("ALTER TABLE `myphpmerge_{$table}` CHANGE `{$row}` `{$row}` VARCHAR(255) NULL DEFAULT NULL;");
                return "GROUP_CONCAT(DISTINCT {$row}) AS {$row}";
            }

            return $maxRow;
        }, $accumColumnsName);
        $accumColumnsName = array_filter($accumColumnsName);

        $table = $this->mergeRule->table;
        $accumColumnsNameOriginal = implode(', ', $accumColumnsNameOriginal);
        $accumColumnsName = implode(', ', $accumColumnsName);
        $uniques = !empty($this->mergeRule->unique) && count($this->mergeRule->unique)
        ? 'GROUP BY ' . implode(', ', $this->mergeRule->unique) : '';

        $count = count($this->mergeRule->unique) ? ', count(myphpmerge__key__) qq' : '';
        $sql = '
                (SELECT ' . $accumColumnsName . '
                    ' . $count . '
                FROM `myphpmerge_' . $table . '`
                ' . $uniques . '
                ORDER BY LPAD(`myphpmerge__key__`, 10, "0") ASC)';

        $sql = sprintf(
            '
            CREATE TABLE `myphpmerge_%1$s_flat` %2$s
            ',
            $table,
            $sql
        );

        $this->groupConnection->execute($sql);

        $this->groupConnection->execute(sprintf(
            'DROP TABLE `myphpmerge_%s`',
            $this->mergeRule->table
        ));

        $this->groupConnection->execute(sprintf(
            'RENAME TABLE `myphpmerge_%s_flat` TO `myphpmerge_%s`;',
            $this->mergeRule->table,
            $this->mergeRule->table
        ));

    }
}
