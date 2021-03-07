package com.ethlo.my2ch.scheduler;

import java.util.UUID;

import com.ethlo.my2ch.My2ch;

public class My2chRunnable implements Runnable
{
    private final My2ch my2ch;
    private final String executionId;

    public My2chRunnable(final My2ch my2ch)
    {
        this.my2ch = my2ch;
        this.executionId = UUID.randomUUID().toString();
    }

    @Override
    public void run()
    {
        my2ch.run(queryProgress -> true);
    }

    public String getExecutionId()
    {
        return executionId;
    }
}
