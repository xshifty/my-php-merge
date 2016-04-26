<?php
namespace Xshifty\MyPhpMerge\Merge\Rules;

use Xshifty\MyPhpMerge\Schema\MysqlConnection;

interface Create
{
    public function build(MysqlConnection $groupConnection, $config);
}
