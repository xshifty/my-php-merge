<?php
namespace Xshifty\MyPhpMerge\Actions;

use Xshifty\MyPhpMerge\Merge\Rules\Rule;
use Xshifty\MyPhpMerge\Schema\MysqlConnection;

final class ApplyCreateRules implements Action
{
    private $createRule;
    private $groupConnection;
    private $config;

    public function __construct(
        Rule $createRule,
        MysqlConnection $groupConnection,
        $config
    )
    {
        $this->createRule = $createRule;
        $this->groupConnection = $groupConnection;
        $this->config = $config;
    }

    public function execute()
    {
        echo '.';
        $this->createRule->build(
            $this->groupConnection,
            $this->config
        );
    }
}
