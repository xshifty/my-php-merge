<?php
namespace Xshifty\MyPhpMerge\Merge\Rules;

use Xshifty\MyPhpMerge\Schema\MysqlConnection;

abstract class MergeRule implements Rule
{
    protected $templaceConnection;
    public $priority = 1;

    public function __construct(MysqlConnection $templateConnection)
    {
        if (empty($this->table)) {
            $reflection = new \ReflectionClass(get_class($this));
            $this->table = strtolower($reflection->getShortName());
        }

        $this->templateConnection = $templateConnection;
    }


    public function getTableColumns()
    {
        return $this->templateConnection->query('DESCRIBE ' . $this->table);
    }
}
