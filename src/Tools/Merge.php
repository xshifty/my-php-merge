<?php
namespace Xshifty\MyPhpMerge\Tools;

use \Xshifty\MyPhpMerge\Schema\MysqlConnection;
use \Xshifty\MyPhpMerge\Schema\MysqlConnectionPDO;
use \Xshifty\MyPhpMerge\Schema\Table\TableAssembler;
use \Xshifty\MyPhpMerge\Merge\Rules\RuleContainer;
use \Xshifty\MyPhpMerge\Merge\Rules\Rule;


final class Merge
{
    private $config;
    private $rules;
    private $ruleContainer;
    private $sourceConnections = [];
    private $templateConnection;
    private $groupConnection;
    private $tableAssembler;
    private $tables = [];

    public function __construct($config, RuleContainer $ruleContainer)
    {
        $this->config = $config;
        $this->ruleContainer = $ruleContainer;
        $this->rules = iterator_to_array(clone($ruleContainer));
    }

    public function setup()
    {
        $this->loadConnections();

        if (!empty($this->config->tables) && count($this->config->tables)) {
            $this->tables = $this->config->tables;
        }
    }

    public function run()
    {
        $this->groupConnection->execute('SET FOREIGN_KEY_CHECKS:=0');

        echo "Preparing merge tables";
        $this->foreachRule([$this, 'prepareTable']);
        echo PHP_EOL;

        echo "Accumulating table data";
        $this->foreachRule([$this, 'accumulateMergeData']);
        echo PHP_EOL;

        echo "Updating primary keys";
        $this->foreachRule([$this, 'updatePrimaryKeys']);
        echo PHP_EOL;

        echo "Updating foreign keys";
        $this->foreachRule([$this, 'updateForeignKeys']);
        echo PHP_EOL;

        echo "Applying create rules";
        $this->foreachRule([$this, 'applyCreateRules']);
        echo PHP_EOL;

        echo "Moving data";
        $this->foreachRule([$this, 'moveMergeData']);
        echo PHP_EOL;

        echo "Cleaning residual data";
        $this->foreachRule([$this, 'cleanUp']);
        echo PHP_EOL;

        $this->groupConnection->execute('SET FOREIGN_KEY_CHECKS:=0');
    }

    private function loadConnections()
    {
        $this->templateConnection = new MysqlConnectionPDO(
            (array) $this->config->schemas->template
        );

        $this->groupConnection = new MysqlConnectionPDO(
            (array) $this->config->schemas->group,
            true
        );

        foreach ($this->config->schemas->source as $source) {
            $this->sourceConnections[$source->schema] = new MysqlConnectionPDO((array) $source);
        }

        if (!$this->groupConnection->schemaExists()) {
            $this->groupConnection->createSchema();
        }

        if (!$this->tableAssembler) {
            $this->tableAssembler = new TableAssembler($this->templateConnection);
        }
    }

    private function foreachRule($callback)
    {
        $rules = $this->rules();

        foreach ($rules as $rule) {
            call_user_func($callback, $rule);
        }
    }

    private function foreachSourceConnection($callback)
    {
        foreach ($this->sourceConnections as $connection) {
            call_user_func($callback, $connection);
        }
    }

    private function prepareTable($rule)
    {
        if (!in_array(RuleContainer::MERGE_INTERFACE, class_implements(get_class($rule)))) {
            return;
        }

        $prepareTableAction = new \Xshifty\MyPhpMerge\Actions\PrepareTable(
            $rule,
            $this->templateConnection,
            $this->groupConnection
        );

        $prepareTableAction->execute();
    }

    private function accumulateMergeData(Rule $rule)
    {
        if (!in_array(RuleContainer::MERGE_INTERFACE, class_implements(get_class($rule)))) {
            return;
        }

        $tableConfig = array_reduce($this->tables, function ($initial, $current) use ($rule) {
            if ($current->table == $rule->table) {
                $initial = $current;
            }

            return $initial;
        });

        if (!$tableConfig) {
            $this->foreachSourceConnection(\Closure::bind(function ($sourceConnection) use ($rule) {
                $accumulateAction = new \Xshifty\MyPhpMerge\Actions\AccumulateMergeData(
                    $rule,
                    $sourceConnection,
                    $this->groupConnection
                );

                $accumulateAction->execute();
            }, $this));

            return;
        }

        if (!empty($tableConfig->sources)) {
            array_map(\Closure::bind(function ($customSource) use ($rule) {
                if (isset($this->sourceConnections[$customSource])) {
                    $accumulateAction = new \Xshifty\MyPhpMerge\Actions\AccumulateMergeData(
                            $rule,
                            $this->sourceConnections[$customSource],
                            $this->groupConnection
                    );

                    $accumulateAction->execute();
                }
            }, $this), $tableConfig->sources);
        }
    }

    private function updatePrimaryKeys(Rule $rule)
    {
        if (!in_array(RuleContainer::MERGE_INTERFACE, class_implements(get_class($rule)))) {
            return;
        }

        $this->foreachSourceConnection(\Closure::bind(function (MysqlConnection $sourceConnection) use ($rule) {
            $updatePrimaryKeysAction = new \Xshifty\MyPhpMerge\Actions\UpdatePrimaryKeys(
                $this->tableAssembler->assembly($rule),
                $sourceConnection,
                $this->groupConnection,
                $this->ruleContainer
            );

            $updatePrimaryKeysAction->execute();
        }, $this));
    }

    private function UpdateForeignKeys(Rule $rule)
    {
        if (!in_array(RuleContainer::MERGE_INTERFACE, class_implements(get_class($rule)))) {
            return;
        }

        $this->foreachSourceConnection(\Closure::bind(function (MysqlConnection $sourceConnection) use ($rule) {
            $updateForeignKeysAction = new \Xshifty\MyPhpMerge\Actions\UpdateForeignKeys(
                $this->tableAssembler->assembly($rule),
                $sourceConnection,
                $this->groupConnection,
                $this->ruleContainer
            );

            $updateForeignKeysAction->execute();
        }, $this));
    }

    private function applyCreateRules(Rule $rule)
    {
        if (!in_array(RuleContainer::CREATE_INTERFACE, class_implements(get_class($rule)))) {
            return;
        }

        $applyCreateRulesAction = new \Xshifty\MyPhpMerge\Actions\ApplyCreateRules(
            $rule,
            $this->groupConnection,
            $this->config
        );

        $applyCreateRulesAction->execute();
    }

    private function moveMergeData(Rule $rule)
    {
        if (!in_array(RuleContainer::MERGE_INTERFACE, class_implements(get_class($rule)))) {
            return;
        }

        $this->foreachSourceConnection($closure = \Closure::bind(function (MysqlConnection $sourceConnection) use ($rule) {
            $moveAction = new \Xshifty\MyPhpMerge\Actions\MoveMergeData(
                $rule,
                $sourceConnection,
                $this->groupConnection
            );

            $moveAction->execute();
        }, $this, $this));
    }

    private function cleanUp(Rule $rule)
    {
        $cleanUpAction = new \Xshifty\MyPhpMerge\Actions\CleanUp(
            $rule,
            $this->groupConnection
        );

        $cleanUpAction->execute();
    }

    private function rules()
    {
        return $this->rules;
    }
}
