package com.ethlo.my2ch;

import java.time.Duration;
import java.time.OffsetDateTime;

public class TransferStatistics
{
    private final long rows;
    private final OffsetDateTime started;
    private final Duration elapsed;

    public TransferStatistics(final long rows, final OffsetDateTime started, final Duration elapsed)
    {
        this.rows = rows;
        this.started = started;
        this.elapsed = elapsed;
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

    public double getRowsPerSecond()
    {
        return rows / ((double) elapsed.toNanos() / 1_000_000);
    }
}
