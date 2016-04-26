<?php
namespace Xshifty\MyPhpMerge\Merge\Rules;

use Xshifty\MyPhpMerge\Schema\MysqlConnection;

abstract class CreateRule implements Rule, Create
{
    protected $templateConnection;
    protected $groupConnection;
    public $table;
    public $priority = 1;

    public function __construct(
        MysqlConnection $templateConnection,
        MysqlConnection $groupConnection
    )
    {
        if (empty($this->table)) {
            $reflection = new \ReflectionClass(get_class($this));
            $this->table = strtolower($reflection->getShortName());
        }

        $this->templateConnection = $templateConnection;
        $this->groupConnection = $groupConnection;
    }

    public function getPriority()
    {
        return $this->priority;
    }
}
