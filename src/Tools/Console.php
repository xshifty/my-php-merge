<?php
namespace Xshifty\MyPhpMerge\Tools;

use \Symfony\Component\Console\Output\OutputInterface;
use \Symfony\Component\Console\Input\InputInterface;
use \Xshifty\MyPhpMerge\Output as Out;
use \Xshifty\MyPhpMerge\Merge\Rules\RuleContainer;
use \Xshifty\MyPhpMerge\Merge\Rules\Rule;
use \Xshifty\MyPhpMerge\Schema\MysqlConnectionPDO;

final class Console
{
    private $mergeFile;
    private $config;
    private $rules;
    private $input;

    private $templateConnection;
    private $sourceConnection;
    private $groupConnection;

    public function __construct($mergeFile = 'Mergefile.json', array $input)
    {
        $this->mergeFile = $mergeFile;
        $this->input = $input;

        $this->loadConfig();
    }

    public function run()
    {
        if (empty($this->input[1]) || $this->input[1] == 'help') {
            return $this->help();
        }

        if ($this->input[1] == 'merge') {
            $this->setupConnection();
            return $this->merge();
        }

        throw new \RuntimeException("Invalid parameter {$this->input[1]}.");
    }

    public function help()
    {
        Out::writeln(PHP_EOL . "\tMy PHP Merge" . PHP_EOL);
        Out::writeln("\t<info>help</info>\t\t\t\t<comment>Show this menu.</comment>");
        Out::writeln('');
    }

    public function merge()
    {
        $this->loadRules();
        foreach ($this->rules as $rule) {
            Out::write(sprintf(
                "<info>[Priority: %d] Merging %s</info>... ",
                $rule->priority,
                $rule->table
            ));
            $this->applyMerge($rule);
            Out::writeln("DONE!");
        }
    }

    private function hasMergefile()
    {
        if (!empty($this->mergeFile)) {
            realpath($this->mergeFile);
        }

        return !empty($this->mergeFile)
            && file_exists($this->mergeFile)
            && is_readable($this->mergeFile);
    }

    private function loadConfig()
    {
        if (!$this->hasMergefile()) {
            throw new \RuntimeException($this->mergeFile . " not found or can't be read.");
        }

        $this->config = json_decode(file_get_contents($this->mergeFile));
        $this->config->path->{'merge-rules'} = empty($this->config->path->{'merge-rules'})
            ? null : realpath($this->config->path->{'merge-rules'});

        if (empty($this->config->path->{'merge-rules'})) {
            throw new \RuntimeException("A valid path.merge-rules can not be found in {$this->mergeFile}");
        }

        if (empty($this->config->{'rules-namespace'})) {
            throw new \RuntimeException("You must defined a valid rules-namespace in {$this->mergeFile}");
        }
    }

    private function loadRules()
    {
        $this->rules = new RuleContainer();

        $rules = glob(sprintf(
            '%s/*.php',
            $this->config->path->{'merge-rules'}
        ));

        foreach ($rules as $file) {
            include $file;
            $class = $this->config->{'rules-namespace'} . '\\' . basename($file, '.php');
            $this->addRule($class);
        }
    }

    private function addRule($class)
    {
        if (
            !in_array(
                'Xshifty\\MyPhpMerge\\Merge\\Rules\\Rule',
                class_implements($class)
            )
        ) {
            throw new \RuntimeException("Rule {$class} must implements Xshifty\\MyPhpMerge\\Merge\\Rules\\Rule");
        }

        return $this->rules->insert(new $class($this->templateConnection));
    }

    private function setupConnection()
    {
            $this->templateConnection = new MysqlConnectionPDO(
                [
                    'host' => $this->config->schemas->template->host,
                    'port' => $this->config->schemas->template->port,
                    'schema' => $this->config->schemas->template->schema,
                ],
                $this->config->schemas->template->user,
                $this->config->schemas->template->password
            );

            $this->groupConnection = new MysqlConnectionPDO(
                [
                    'host' => $this->config->schemas->group->host,
                    'port' => $this->config->schemas->group->port,
                    'schema' => $this->config->schemas->group->schema,
                ],
                $this->config->schemas->group->user,
                $this->config->schemas->group->password,
                true
            );

            if (!$this->groupConnection->schemaExists()) {
                $this->groupConnection->createSchema();
            }
    }

    private function applyMerge(Rule $rule)
    {
        $this->groupConnection->execute(sprintf(
            'DROP TABLE IF EXISTS %s',
            $rule->table
        ));

        $this->groupConnection->execute(sprintf(
            'DROP TABLE IF EXISTS myphpmerge_%s',
            $rule->table
        ));

        $copySucceed = $this->groupConnection->copyTable(
            $rule->table,
            $this->templateConnection
        );

        if (!$copySucceed) {
            throw new \RuntimeException("Can't copy {$this->config->schemas->template->database} table {$rule->table} to {$this->config->schemas->group->database}");
        }

        $this->groupConnection->execute(sprintf(
            'CREATE TABLE myphpmerge_%1$s (SELECT * FROM %1$s)',
            $rule->table
        ));

        if (!$this->groupConnection->hasTable('myphpmerge_' . $rule->table)) {
            throw new \RuntimeException("Can't be possible create transitional table for table {$rule->table}");
        }

        $alterSql = sprintf('
            ALTER TABLE myphpmerge_%1$s
                ADD COLUMN myphpmerge_%2$s BIGINT UNSIGNED NOT NULL AUTO_INCREMENT FIRST,
                ADD PRIMARY KEY (myphpmerge_%2$s)
            ',
            $rule->table,
            $rule->primaryKey
        );

        $this->groupConnection->execute($alterSql);
        foreach ($this->config->schemas->source as $sourceConfig)
        {
            $sourceConnection = new MysqlConnectionPDO(
                [
                    'host' => $sourceConfig->host,
                    'port' => $sourceConfig->port,
                    'schema' => $sourceConfig->schema,
                ],
                $sourceConfig->user,
                $sourceConfig->password
            );

            $accumulateAction = new \Xshifty\MyPhpMerge\Actions\AccumulateMergeData(
                $rule,
                $sourceConnection,
                $this->groupConnection
            );

            $moveAction = new \Xshifty\MyPhpMerge\Actions\MoveMergeData(
                $rule,
                $sourceConnection,
                $this->groupConnection
            );

            $accumulateAction->execute();
            $moveAction->execute();
        }
        $this->groupConnection->execute(sprintf(
            'DROP TABLE IF EXISTS myphpmerge_%s',
            $rule->table
        ));
    }
}
