package com.ethlo.my2ch;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.ethlo.my2ch.scheduler.My2chScheduler;

@Configuration
public class My2chCfg
{
    @Bean
    public My2chScheduler scheduler()
    {
        return new My2chScheduler(1);
    }
}
