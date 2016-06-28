<?php
namespace Xshifty\MyPhpMerge\Tools;

use \Xshifty\MyPhpMerge\Merge\Rules\Rule;
use \Xshifty\MyPhpMerge\Merge\Rules\RuleContainer;
use \Xshifty\MyPhpMerge\Schema\MysqlConnection;
use \Xshifty\MyPhpMerge\Schema\MysqlConnectionPDO;
use \Xshifty\MyPhpMerge\Schema\Table\TableAssembler;

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

    public function __construct(
        MysqlConnection $templateConnection,
        MysqlConnection $groupConnection,
        $config,
        RuleContainer $ruleContainer
    ) {
        $this->templateConnection = $templateConnection;
        $this->groupConnection = $groupConnection;
        $this->config = $config;
        $this->ruleContainer = $ruleContainer;
        $this->rules = iterator_to_array(clone ($ruleContainer));
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

        cprint("<info>Preparing merge tables</info>");
        $this->foreachRule([$this, 'prepareTable']);
        echo PHP_EOL;

        cprint("<info>Accumulating table data</info>");
        $this->foreachRule([$this, 'accumulateMergeData']);
        echo PHP_EOL;

        cprint("<info>Updating primary keys</info>");
        $this->foreachRule([$this, 'updatePrimaryKeys']);
        echo PHP_EOL;

        cprint("<info>Preparing foreign keys</info>");
        $this->foreachRule([$this, 'prepareForeignKeys']);
        echo PHP_EOL;

        cprint("<info>Flatting table data</info>");
        $this->foreachRule([$this, 'flatDuplicateData']);
        echo PHP_EOL;

        cprint("<info>Updating foreign keys</info>");
        $this->foreachRule([$this, 'updateForeignKeys']);
        echo PHP_EOL;

        cprint("<info>Flatting table data</info>");
        $this->foreachRule([$this, 'flatDuplicateDataSecondFactor']);
        echo PHP_EOL;

        cprint("<info>Updating foreign keys</info>");
        $this->foreachRule([$this, 'updateForeignKeys']);
        echo PHP_EOL;

        cprint("<info>Moving data</info>");
        $this->foreachRule([$this, 'moveMergeData']);
        echo PHP_EOL;

        cprint("<info>Applying create rules</info>");
        $this->foreachRule([$this, 'applyCreateRules']);
        echo PHP_EOL;

        cprint("<info>Cleaning residual data</info>");
        $this->foreachRule([$this, 'cleanUp']);
        echo PHP_EOL;

        $this->groupConnection->execute('SET FOREIGN_KEY_CHECKS:=1');
    }

    private function loadConnections()
    {
        foreach ($this->config->schemas->source as $source) {
            $this->sourceConnections[$source->schema] = new MysqlConnectionPDO((array) $source);
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

        $updatePrimaryKeysAction = new \Xshifty\MyPhpMerge\Actions\UpdatePrimaryKeys(
            $this->tableAssembler->assembly($rule),
            $this->groupConnection,
            $this->ruleContainer
        );

        $updatePrimaryKeysAction->execute();
    }

    private function UpdateForeignKeys(Rule $rule)
    {
        if (!in_array(RuleContainer::MERGE_INTERFACE, class_implements(get_class($rule)))) {
            return;
        }

        $updateForeignKeysAction = new \Xshifty\MyPhpMerge\Actions\UpdateForeignKeys(
            $this->tableAssembler->assembly($rule),
            $this->groupConnection,
            $this->ruleContainer
        );

        $updateForeignKeysAction->execute();
    }

    private function PrepareForeignKeys(Rule $rule)
    {
        if (!in_array(RuleContainer::MERGE_INTERFACE, class_implements(get_class($rule)))) {
            return;
        }

        $prepareForeignKeysAction = new \Xshifty\MyPhpMerge\Actions\PrepareForeignKeys(
            $this->tableAssembler->assembly($rule),
            $this->groupConnection,
            $this->ruleContainer
        );

        $prepareForeignKeysAction->execute();
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

    private function flatDuplicateData(Rule $rule)
    {

        $moveAction = new \Xshifty\MyPhpMerge\Actions\FlatDuplicateData(
            $rule,
            $this->groupConnection
        );

        $moveAction->execute();
    }

    private function flatDuplicateDataSecondFactor(Rule $rule)
    {

        $moveAction = new \Xshifty\MyPhpMerge\Actions\FlatDuplicateDataSecondFactor(
            $rule,
            $this->groupConnection
        );

        $moveAction->execute();
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
