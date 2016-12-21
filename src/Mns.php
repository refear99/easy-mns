<?php

namespace Refear99\EasyMns;

use AliyunMNS\Client as MnsClient;

class Mns
{
    protected $mns;

    public function __construct()
    {
        $config = config('mns');

        $this->mns = new MnsClient($config['endpoint'], $config['key'], $config['secret']);
    }
}