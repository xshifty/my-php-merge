<?php
namespace Xshifty\MyPhpMerge\Schema\Table;

abstract class ColumnBase
{
    protected $name;
    protected $table;
    protected $type;

    public function __construct($table, $name, $type)
    {
        $this->table = $table;
        $this->name = $name;
        $this->type = $type;
    }

    public function getTable()
    {
        return $this->table;
    }

    public function getFullName()
    {
        return sprintf('%s.%s', $this->getTable(), $this->getName());
    }

    public function getName()
    {
        return $this->name;
    }

    public function getRawType()
    {
        $type = explode(' ', $this->type);
        return reset($type);
    }

    public function getType()
    {
        $type = $this->getRawType();

        if (preg_match('/INTEGER|INT|SMALLINT|TINYINT|MEDIUMINT|BIGINT/', strtoupper($type))) {
            return 'INTEGER';
        }

        if (preg_match('/DECIMAL|NUMERIC|FLOAT|DOUBLE/', strtoupper($type))) {
            return 'FLOAT';
        }

        if (preg_match('/DATE|TIME|DATETIME|TIMESTAMP|YEAR/', strtoupper($type))) {
            return 'TIME';
        }

        if (preg_match('/CHAR|VARCHAR|BLOB|TEXT|TINYBLOB|TINYTEXT|MEDIUMBLOB|MEDIUMTEXT|LONGBLOB|LONGTEXT|ENUM/', strtoupper($type))) {
            return 'STRING';
        }

        return 'UNKNOWN';
    }
}
