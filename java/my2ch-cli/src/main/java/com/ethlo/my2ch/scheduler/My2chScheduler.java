package com.ethlo.my2ch.scheduler;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.task.TaskSchedulerBuilder;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.ethlo.my2ch.My2ch;
import com.ethlo.my2ch.config.Schedule;
import com.ethlo.my2ch.config.TransferConfig;

public class My2chScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(My2chScheduler.class);
    private final ThreadPoolTaskScheduler taskScheduler;

    public My2chScheduler(final int poolSize)
    {
        this.taskScheduler = new TaskSchedulerBuilder()
                .threadNamePrefix("my2ch-")
                .poolSize(poolSize)
                .build();
        taskScheduler.initialize();
    }

    public void runAtInterval(final My2ch task, final Duration interval)
    {
        taskScheduler.scheduleWithFixedDelay(() ->
        {
            final NumberFormat nf = NumberFormat.getInstance(Locale.getDefault());
            nf.setGroupingUsed(true);
            nf.setMaximumFractionDigits(2);

            logger.info("Running task {}", task.getConfig().getAlias());
            final long started = System.nanoTime();
            final long rowCount = task.run(progress ->
            {
                logger.info("Copy in progress: {}", nf.format(progress.getReadRows()));
                return true;
            });
            final long elapsed = System.nanoTime() - started;

            final Map<String, Object> stats = task.getStats(task.getConfig());
            final double rowsPerSec = rowCount / (elapsed / 1_000_000_000D);
            logger.info("Completed task {}. {} new rows in {} ({}/sec). {} total rows. Last modified {}",
                    task.getConfig().getAlias(), nf.format(rowCount), Duration.ofNanos(elapsed), nf.format(rowsPerSec), nf.format(stats.get("rows")), stats.get("last_modified")
            );
        }, interval);
    }

    public void runAtInterval(final TransferConfig config)
    {
        final Optional<Duration> interval = Optional.ofNullable(config.getSchedule()).map(Schedule::getInterval);
        if (interval.isPresent())
        {
            logger.info("Scheduling {} with interval {}", config.getAlias(), interval.get());
            runAtInterval(new My2ch(config), interval.get());
        }
    }

    public void shutdown()
    {
        taskScheduler.setWaitForTasksToCompleteOnShutdown(true);
        taskScheduler.shutdown();
    }
}
