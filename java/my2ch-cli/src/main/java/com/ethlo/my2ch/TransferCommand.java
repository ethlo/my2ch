package com.ethlo.my2ch;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ethlo.my2ch.My2ch;
import com.ethlo.my2ch.My2chConfigLoader;
import com.ethlo.my2ch.config.TransferConfig;
import picocli.CommandLine;

@Component
@CommandLine.Command(name = "transfer")
public class TransferCommand implements Callable<Long>
{
    private static final Logger logger = LoggerFactory.getLogger(TransferCommand.class);

    @CommandLine.Option(names = "--names", description = "The name of config(s) to run. Undefined runs all")
    private List<String> names;

    @CommandLine.Option(names = "--home", description = "The base directory to read config files from", required = true)
    private Path home;

    @CommandLine.Option(names = "--service", description = "Run as background-service")
    private Boolean service;

    public Long call()
    {
        long total = 0;
        if (names != null && !names.isEmpty())
        {
            for (String name : names)
            {
                final TransferConfig config = My2chConfigLoader.loadConfig(home, name);
                total += processSingle(config);
            }
        }
        else
        {
            final List<TransferConfig> configs = My2chConfigLoader.loadConfigs(home);
            for (final TransferConfig config : configs)
            {
                total += processSingle(config);
            }

        }
        return total;
    }

    private long processSingle(final TransferConfig config)
    {
        logger.info("Processing {}", config.getAlias());
        final My2ch my2ch = new My2ch(config);
        return my2ch.run(queryProgress ->
        {
            logger.info("Rows copied: {}", queryProgress.getReadRows());
            return true;
        });
    }
}