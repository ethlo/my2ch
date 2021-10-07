package com.ethlo.my2ch;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.ethlo.my2ch.scheduler.My2chTaskRunner;

@Configuration
public class My2chCfg
{
    @Bean
    public My2chTaskRunner scheduler()
    {
        return new My2chTaskRunner(1);
    }
}
