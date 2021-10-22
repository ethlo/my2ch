package com.ethlo.my2ch.actuator;

import java.util.List;
import java.util.Map;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

import com.ethlo.my2ch.scheduler.My2chTaskRunner;

@Component
@Endpoint(id = "transfers")
public class TransferStatusEndpoint
{
    private final My2chTaskRunner scheduler;

    public TransferStatusEndpoint(final My2chTaskRunner scheduler)
    {
        this.scheduler = scheduler;
    }

    @ReadOperation
    public List<Map<String, Object>> status()
    {
        return scheduler.getTaskStatuses();
    }
}
