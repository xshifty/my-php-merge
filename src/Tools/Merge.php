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

    public function __construct($config, RuleContainer $ruleContainer)
    {
        $this->config = $config;
        $this->ruleContainer = $ruleContainer;
        $this->rules = iterator_to_array(clone($ruleContainer));
    }

    public function setup()
    {
        $this->loadConnections();
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

        echo "Updating primary/foreign keys";
        $this->foreachRule([$this, 'updateKeys']);
        echo PHP_EOL;

        echo "Moving data";
        $this->foreachRule([$this, 'moveMergeData']);
        echo PHP_EOL;

        /*
        echo "Cleaning residual data";
        $this->foreachRule([$this, 'cleanUp']);
        echo PHP_EOL;
        */

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
            array_push(
                $this->sourceConnections,
                new MysqlConnectionPDO((array) $source)
            );
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
        $prepareTableAction = new \Xshifty\MyPhpMerge\Actions\PrepareTable(
            $rule,
            $this->templateConnection,
            $this->groupConnection
        );

        $prepareTableAction->execute();
    }

    private function accumulateMergeData(Rule $rule)
    {
        $this->foreachSourceConnection(\Closure::bind(function ($sourceConnection) use ($rule) {
            $accumulateAction = new \Xshifty\MyPhpMerge\Actions\AccumulateMergeData(
                $rule,
                $sourceConnection,
                $this->groupConnection
            );

            $accumulateAction->execute();
        }, $this, $this));
    }

    private function updateKeys(Rule $rule)
    {
        $this->foreachSourceConnection(\Closure::bind(function (MysqlConnection $sourceConnection) use ($rule) {
            $updateKeysAction = new \Xshifty\MyPhpMerge\Actions\UpdateKeys(
                $this->tableAssembler->assembly($rule),
                $sourceConnection,
                $this->groupConnection,
                $this->ruleContainer
            );

            $updateKeysAction->execute();
        }, $this, $this));
    }

    private function moveMergeData(Rule $rule)
    {
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
