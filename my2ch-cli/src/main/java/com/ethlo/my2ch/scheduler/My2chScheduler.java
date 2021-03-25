package com.ethlo.my2ch.scheduler;

import java.text.NumberFormat;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.task.TaskSchedulerBuilder;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.ethlo.my2ch.My2ch;
import com.ethlo.my2ch.TransferStatistics;
import com.ethlo.my2ch.config.Schedule;
import com.ethlo.my2ch.config.TransferConfig;

public class My2chScheduler implements TaskStatusListener
{
    private static final Logger logger = LoggerFactory.getLogger(My2chScheduler.class);
    private final ThreadPoolTaskScheduler taskScheduler;
    private final Map<String, TransferConfig> tasks = new ConcurrentHashMap<>();
    private final Map<String, TransferStatistics> lastSuccess = new ConcurrentHashMap<>();

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
        this.tasks.put(task.getConfig().getAlias(), task.getConfig());
        taskScheduler.scheduleWithFixedDelay(() ->
        {
            final NumberFormat nf = NumberFormat.getInstance(Locale.getDefault());
            nf.setGroupingUsed(true);
            nf.setMaximumFractionDigits(2);

            logger.info("Running task {}", task.getConfig().getAlias());
            try
            {
                final OffsetDateTime started = OffsetDateTime.now();
                final long rowCount = task.run(progress ->
                {
                    logger.info("Copy in progress: {}", nf.format(progress.getReadRows()));
                    return true;
                });
                final Duration elapsed = Duration.between(started, OffsetDateTime.now());

                final TransferStatistics stats = new TransferStatistics(rowCount, started, elapsed, task.getStats());
                logger.info("Completed task {}. {} new rows in {} ({}/sec). {} total rows. Last modified {}",
                        task.getConfig().getAlias(), nf.format(rowCount), elapsed, nf.format(stats.getRowsPerSecond()), nf.format(stats.getTableStatistics().get("rows")), stats.getTableStatistics().get("last_modified")
                );

                finishedSuccess(task.getConfig().getAlias(), stats);
            }
            catch (Exception exc)
            {
                logger.error(exc.getMessage(), exc);
                finishedError(task.getConfig().getAlias(), exc);
            }
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

    public List<Map<String, Object>> getTaskStatuses()
    {
        final List<Map<String, Object>> result = new LinkedList<>();
        for (String alias : tasks.keySet())
        {
            final Map<String, Object> data = new LinkedHashMap<>();
            data.put("alias", alias);
            data.put("last_run", lastSuccess.get(alias));
            result.add(data);
        }
        return result;
    }

    @Override
    public void finishedSuccess(final String task, final TransferStatistics transferStatistics)
    {
        lastSuccess.put(task, transferStatistics);
    }

    @Override
    public void finishedError(final String task, final Exception exception)
    {

    }
}
