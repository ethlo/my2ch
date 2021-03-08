package com.ethlo.my2ch;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TransferStatistics
{
    private final long rows;
    private final OffsetDateTime started;
    private final Duration elapsed;
    private final Map<String, Object> tableStatistics;

    public TransferStatistics(final long rows, final OffsetDateTime started, final Duration elapsed, final Map<String, Object> tableStatistics)
    {
        this.rows = rows;
        this.started = started;
        this.elapsed = elapsed;
        this.tableStatistics = tableStatistics;
    }

    public long getRows()
    {
        return rows;
    }

    public OffsetDateTime getStarted()
    {
        return started;
    }

    public Duration getElapsed()
    {
        return elapsed;
    }

    @JsonProperty("rows_per_second")
    public double getRowsPerSecond()
    {
        return rows / ((double) elapsed.toNanos() / 1_000_000);
    }

    @JsonProperty("table_stats")
    public Map<String, Object> getTableStatistics()
    {
        return tableStatistics;
    }
}
