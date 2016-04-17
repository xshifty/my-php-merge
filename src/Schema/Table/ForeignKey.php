<?php
namespace Xshifty\MyPhpMerge\Schema\Table;

final class ForeignKey extends ColumnBase
{
    private $parentTable;
    private $parentColumn;

    public function setParentTable($parentTable)
    {
        $this->parentTable = $parentTable;
    }

    public function getParentTable()
    {
        return $this->parentTable;
    }

    public function setParentColumn($parentColumn)
    {
        $this->parentColumn = $parentColumn;
    }

    public function getParentColumn()
    {
        return $this->parentColumn;
    }
}
