<?php
namespace Xshifty\MyPhpMerge;

use \Symfony\Component\Console\Output\OutputInterface;
use \Symfony\Component\Console\Input\InputInterface;

final class Console
{
    private $mergeFile;
    private $config;
    private $groupConn;
    private $templateConn;
    private $sourceConn;
    private $output;
    private $input;

    public function __construct($mergeFile = 'Mergefile.json', array $input, OutputInterface $output)
    {
        $this->mergeFile = $mergeFile;
        $this->output = $output;
        $this->input = $input;
        $this->loadConfig();
    }

    public function run()
    {
        if (empty($this->input[1]) || $this->input[1] == 'help') {
            $this->help();
        }
    }

    public function help()
    {
        $this->output->writeln(PHP_EOL . "\tMy PHP Merge" . PHP_EOL);
        $this->output->writeln("\t<info>help</info>\t\t\t\t<comment>Show this menu.</comment>");
        $this->output->writeln('');
    }

    private function hasMergefile()
    {
        return file_exists($this->mergeFile) && is_readable($this->mergeFile);
    }

    private function loadConfig()
    {
        if (!$this->hasMergefile()) {
            throw new \RuntimeException($this->mergeFile . " not found or can't be read.");
        }

        $this->config = json_decode(file_get_contents($this->mergeFile));
    }
}
