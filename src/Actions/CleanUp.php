<?php
namespace Xshifty\MyPhpMerge\Actions;

use Xshifty\MyPhpMerge\Merge\Rules\MergeRule;
use Xshifty\MyPhpMerge\Schema\MysqlConnection;

final class CleanUp implements Action
{
    private $mergeRule;
    private $groupConnection;

    public function __construct(
        MergeRule $mergeRule,
        MysqlConnection $groupConnection
    )
    {
        $this->mergeRule = $mergeRule;
        $this->groupConnection = $groupConnection;
    }

    public function execute()
    {
        echo '.';
        $this->groupConnection->execute('DROP TABLE IF EXISTS myphpmerge_' . $this->mergeRule->table);
    }
}
