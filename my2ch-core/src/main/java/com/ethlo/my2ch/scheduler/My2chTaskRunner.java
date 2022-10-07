package com.ethlo.my2ch.scheduler;

/*-
 * #%L
 * Simple MySQL to ClickHouse ETL - CLI
 * %%
 * Copyright (C) 2018 - 2022 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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

public class My2chTaskRunner implements TaskStatusListener
{
    private static final Logger logger = LoggerFactory.getLogger(My2chTaskRunner.class);
    private final ThreadPoolTaskScheduler taskScheduler;
    private final Map<String, TransferConfig> tasks = new ConcurrentHashMap<>();
    private final Map<String, TransferStatistics> lastSuccess = new ConcurrentHashMap<>();

    public My2chTaskRunner(final int poolSize)
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
            try
            {
                runTask(task);
            }
            catch (Exception exc)
            {
                logger.error(exc.getMessage(), exc);
                finishedError(task.getConfig().getAlias(), exc);
            }
        }, interval);
    }

    public TransferStatistics runTask(My2ch task)
    {
        logger.info("Task {} - Starting", task.getConfig().getAlias());
        final OffsetDateTime started = OffsetDateTime.now();
        final long rowCount = task.run(progress ->
        {
            logger.info("Task {} - Progress {}", task.getConfig().getAlias(), format(progress.getReadRows()));
            return true;
        });
        final Duration elapsed = Duration.between(started, OffsetDateTime.now());

        final TransferStatistics stats = new TransferStatistics(rowCount, started, elapsed, task.getStats());
        logger.info("Task {} - Completed with {} new rows in {} ({}/sec). {} total rows. Last modified {}",
                task.getConfig().getAlias(), format(rowCount), elapsed, format(stats.getRowsPerSecond()), format(stats.getTableStatistics().get("rows")), stats.getTableStatistics().get("last_modified")
        );

        finishedSuccess(task.getConfig().getAlias(), stats);

        return stats;
    }

    private String format(Object obj)
    {
        if (obj == null)
        {
            return null;
        }

        final NumberFormat nf = NumberFormat.getInstance(Locale.getDefault());
        nf.setGroupingUsed(true);
        nf.setMaximumFractionDigits(2);
        try
        {
            return nf.format(obj);
        }
        catch (IllegalArgumentException exc)
        {
            logger.info("Unable to format {}", obj, exc);
            return "0";
        }
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

    public Optional<TransferConfig> getTask(String name)
    {
        return Optional.ofNullable(tasks.get(name));
    }

    public Map<String, TransferConfig> getTasks()
    {
        return tasks;
    }
}
