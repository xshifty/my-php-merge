<?php
namespace Xshifty\MyPhpMerge\Schema\Table;

abstract class TableBase
{
    protected $name;
    protected $columns;

    public function __construct($name)
    {
        $this->name = $name;
        $this->columns = new \ArrayObject();
    }

    public function getName()
    {
        return $this->name;
    }

    public function addColumn(ColumnBase $column)
    {
        if (!$this->hasColumn($column)) {
            $this->columns->append($column);
        }

        return true;
    }

    public function hasColumn(ColumnBase $column)
    {
        return in_array(
            $column->getName(),
            array_map(function (ColumnBase $column) {
                return $column->getName();
            }, $this->columns->getArrayCopy())
        );
    }

    public function getPrimaryKey()
    {
        return array_reduce($this->columns->getArrayCopy(), function ($first, $current) {
            if (get_class($current) == 'Xshifty\\MyPhpMerge\\Schema\\Table\\PrimaryKey') {
                $first = $current;
            }

            return $first;
        });
    }

    public function getForeignKeys()
    {
        return array_filter($this->columns->getArrayCopy(), function ($column) {
            if (get_class($column) == 'Xshifty\\MyPhpMerge\\Schema\\Table\\ForeignKey') {
                return $column;
            }

            return false;
        });
    }
}
