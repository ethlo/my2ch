package com.ethlo.my2ch;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ethlo.my2ch.config.TransferConfig;
import com.ethlo.my2ch.scheduler.My2chScheduler;
import picocli.CommandLine;

@Component
@CommandLine.Command(name = "transfer")
public class TransferCommand implements Callable<Long>
{
    private static final Logger logger = LoggerFactory.getLogger(TransferCommand.class);
    private final My2chScheduler scheduler;

    @CommandLine.Option(names = "--names", description = "The name of config(s) to run. Undefined runs all")
    private List<String> names;

    @CommandLine.Option(names = "--home", description = "The base directory to read config files from", required = true)
    private Path home;

    @CommandLine.Option(names = "--service", description = "Run as background-service")
    private Boolean service;

    public TransferCommand(My2chScheduler scheduler)
    {
        this.scheduler = scheduler;
    }

    public Long call() throws InterruptedException
    {
        final boolean schedule = service != null && service;

        final List<String> aliases = (names != null && !names.isEmpty()) ? names : My2chConfigLoader.getConfigs(home);
        long total = 0;
        for (String alias : aliases)
        {
            final TransferConfig config = My2chConfigLoader.loadConfig(home, alias);
            total += processSingle(config);
            if (schedule)
            {
                scheduler.runAtInterval(config);
            }
        }

        logger.info("Completed with a total of {} copied rows", total);

        if (schedule)
        {
            final CountDownLatch latch = new CountDownLatch(1);
            latch.await();
        }

        return total;
    }

    private long processSingle(final TransferConfig config)
    {
        logger.info("Processing {}", config.getAlias());
        try (final My2ch my2ch = new My2ch(config))
        {
            return my2ch.run(queryProgress ->
            {
                logger.info("Rows copied: {}", queryProgress.getReadRows());
                return true;
            });
        }
    }
}