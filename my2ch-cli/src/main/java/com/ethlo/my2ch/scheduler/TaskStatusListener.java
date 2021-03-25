package com.ethlo.my2ch.scheduler;

import com.ethlo.my2ch.TransferStatistics;

public interface TaskStatusListener
{
    void finishedSuccess(String task, TransferStatistics transferStatistics);

    void finishedError(String task, Exception exception);
}
