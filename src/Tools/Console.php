<?php
namespace Xshifty\MyPhpMerge\Tools;

use \Symfony\Component\Console\Output\OutputInterface;
use \Symfony\Component\Console\Input\InputInterface;
use \Xshifty\MyPhpMerge\Output as Out;
use \Xshifty\MyPhpMerge\Merge\Rules\RuleContainer;
use \Xshifty\MyPhpMerge\Merge\Rules\Rule;
use \Xshifty\MyPhpMerge\Schema\MysqlConnectionPDO;
use \Xshifty\MyPhpMerge\Schema\Table\TableAssembler;

final class Console implements Tool
{
    private $includedRules = [];
    private $mergeFile;
    private $config;
    private $rules;
    private $input;
    private $templateConnection;
    private $groupConnection;

    public function __construct($mergeFile = 'Mergefile.json', array $input)
    {
        $this->mergeFile = $mergeFile;
        $this->input = $input;

        $this->setup();
        $this->loadConfig();
    }

    public function run()
    {
        if (empty($this->input[1]) || $this->input[1] == 'help') {
            return $this->help();
        }

        if ($this->input[1] == 'merge') {
            return $this->merge();
        }

        throw new \RuntimeException("Invalid parameter {$this->input[1]}.");
    }

    public function help()
    {
        Out::writeln(PHP_EOL . "\tMy PHP Merge" . PHP_EOL);
        Out::writeln("\t<info>help</info>\t\t\t\t<comment>Show this menu.</comment>");
        Out::writeln("\t<info>merge</info>\t\t\t\t<comment>Start merging process.</comment>\n");
    }

    public function merge()
    {
        $this->loadRules();
        $mergeTool = new Merge($this->config, $this->rules);
        $mergeTool->setup();
        $mergeTool->run();
    }

    public function setup()
    {
        if (!$this->hasMergefile()) {
            throw new \RuntimeException($this->mergeFile . " not found or can't be read.");
        }

        $this->loadConfig();
        $this->loadConnections();
        $this->loadRules();
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

    private function loadConnections()
    {
        $this->templateConnection = new MysqlConnectionPDO(
            (array) $this->config->schemas->template
        );

        $this->groupConnection = new MysqlConnectionPDO(
            (array) $this->config->schemas->group
        );
    }

    private function loadRules()
    {
        $this->rules = new RuleContainer();

        $rules = glob(sprintf(
            '%s/*.php',
            $this->config->path->{'merge-rules'}
        ));

        foreach ($rules as $file) {
            if (!in_array($file, $this->includedRules)) {
                include $file;
                $this->includedRules[] = $file;
            }

            $class = $this->config->{'rules-namespace'} . '\\' . basename($file, '.php');
            $this->addRule($class);
        }
    }

    private function addRule($class)
    {
        if (!in_array('Xshifty\\MyPhpMerge\\Merge\\Rules\\Rule', class_implements($class))) {
            throw new \RuntimeException("Rule {$class} must implements Xshifty\\MyPhpMerge\\Merge\\Rules\\Rule");
        }

        if (in_array('Xshifty\\MyPhpMerge\\Merge\\Rules\\Create', class_implements($class))) {
            return $this->rules->insert(new $class(
                $this->templateConnection,
                $this->groupConnection
            ));
        }

        return $this->rules->insert(new $class(
            $this->templateConnection
        ));
    }
}
