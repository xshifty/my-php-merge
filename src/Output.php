<?php
namespace Xshifty\MyPhpMerge;

final class Output
{
    private static $output;

    public static function setup()
    {
        $warningStyle = new \Symfony\Component\Console\Formatter\OutputFormatterStyle('yellow', null, ['blink', 'bold']);
        self::$output = new \Symfony\Component\Console\Output\ConsoleOutput();
        self::$output->getFormatter()->setStyle('warning', $warningStyle);
    }

    public static function __callStatic($name, $args)
    {
        return call_user_func_array([self::$output, $name], $args);
    }
}
