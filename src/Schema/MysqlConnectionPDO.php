<?php
namespace Xshifty\MyPhpMerge\Schema;

final class MysqlConnectionPDO implements MysqlConnection
{
    private $pdo;
    private $config;

    public function __construct(array $config, $createSchema = false)
    {
        if (empty($config['schema'])) {
            throw new \RuntimeException('Your must define a schema for MysqlConnectionPDO');
        }

        $this->config['host'] = empty($config['host']) ? 'localhost' : $config['host'];
        $this->config['port'] = empty($config['port']) ? 3306 : $config['port'];
        $this->config['schema'] = $config['schema'];
        $this->config['readonly'] = !empty($config['readonly']);

        $dsn = sprintf(
            "mysql:host=%s;port=%s;",
            $config['host'],
            $config['port']
        );

        $user = $config['user'];
        $password = !empty($config['password']) ? $config['password'] : null;
        $this->pdo = new \PDO($dsn, $user, $password);
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        if ($this->config['readonly']) {
            $this->execute('SET @read_only = 1;');
        }

        if (!$this->schemaExists() && (!$createSchema || $this->config['readonly'])) {
            throw new \RuntimeException("Schema {$this->config['schema']} not found.");
        }

        $this->createSchema();
        $this->useSchema();
    }

    public function execute($sql, $parameters = null)
    {
        $stmt = $this->pdo->prepare($sql);
        $succeed = $stmt->execute($parameters);
        return $succeed ? $stmt : false;
    }

    public function query($sql, $parameters = null, $fetchMode = \PDO::FETCH_ASSOC)
    {
        $stmt = $this->execute($sql, $parameters);

        if ($stmt) {
            $result = $stmt->rowCount() ? $stmt->fetchAll($fetchMode) : [];
            return $result;
        }

        return $stmt;
    }

    public function schemaExists()
    {
        return in_array(
            $this->config['schema'],
            $this->query("SHOW SCHEMAS", null, \PDO::FETCH_COLUMN)
        );
    }

    public function createSchema($source = null, $drop = false)
    {
        if ($drop && $this->schemaExists()) {
            $this->execute('DROP DATABASE ' . $this->config['schema']);
        }

        $created = $this->schemaExists();
        if (!$created) {
            $created = $this->execute('CREATE SCHEMA ' . $this->config['schema']);
        }

        if (!$source) {
            return $created;
        }

        return $this->source($source) && $created;
    }

    public function useSchema()
    {
        return $this->execute('USE ' . $this->config['schema']);
    }

    public function hasTable($tableName)
    {
        $tables = $this->query("SHOW TABLES", null, \PDO::FETCH_COLUMN);
        return in_array(
            $tableName,
            $tables
        ) && count($tables);
    }

    public function copyTable($tableName, MysqlConnection $from)
    {
        if ($this->config['readonly']) {
            throw new \RuntimeException("Can't copy table, {$this->config['schema']} is readonly.");
        }

        $createTable = $from->query("SHOW CREATE TABLE `{$tableName}`");
        if (!empty($createTable[0])) {
            $createTable = array_values($createTable[0]);
            $sql = $createTable[1];
        }

        if (empty($sql)) {
            return false;
        }

        if ($this->hasTable($tableName)) {
            return true;
        }

        return $this->execute($sql);
    }

    public function getConfig()
    {
        return (object) $this->config;
    }

    public function quote($string)
    {
        return $this->pdo->quote($string);
    }
}
