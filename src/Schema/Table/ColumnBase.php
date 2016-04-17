<?php
namespace Xshifty\MyPhpMerge\Schema\Table;

abstract class ColumnBase
{
    protected $name;
    protected $table;

    public function __construct(TableBase $table, $name)
    {
        $this->table = $table;
        $this->name = $name;
    }

    public function getTable()
    {
        return $this->table;
    }

    public function getFullName()
    {
        return sprintf('%s.%s', $this->getTable()->getName(), $this->getName());
    }

    public function getName()
    {
        return $this->name;
    }
}
