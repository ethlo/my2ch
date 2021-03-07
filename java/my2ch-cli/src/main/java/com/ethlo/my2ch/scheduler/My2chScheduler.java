package com.ethlo.my2ch.scheduler;

import java.time.Duration;
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
                .threadNamePrefix("my2ch-scheduler")
                .poolSize(poolSize)
                .build();
        taskScheduler.initialize();
    }

    public void runAtInterval(final My2ch task, final Duration interval)
    {
        taskScheduler.scheduleWithFixedDelay(() -> {
            final long copied = task.run(p -> true);
            final Map<String, Object> stats = task.getStats(task.getConfig());
            logger.info("Statistics for {}. {} new rows. {} total rows. Last modified {}", task.getConfig().getAlias(), copied, stats.get("rows"), stats.get("last_modified"));
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
