<?php
namespace Xshifty\MyPhpMerge\Schema;

interface MysqlConnection
{
    public function execute($sql, $parameters = null);
    public function query($sql, $parameters = null, $fetchMode = \PDO::FETCH_ASSOC);
    public function schemaExists();
    public function createSchema($drop = false);
    public function useSchema();
    public function hasTable($tableName);
    public function copyTable($tableName, MysqlConnection $from);
    public function getConfig();
    public function quote($string);
}
