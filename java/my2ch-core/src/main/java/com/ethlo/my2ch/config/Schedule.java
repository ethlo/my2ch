package com.ethlo.my2ch.config;

import java.time.Duration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;

@Valid
public class Schedule
{
    @NotNull
    private final Duration interval;

    public Schedule(@JsonProperty("interval") final Duration interval)
    {
        this.interval = interval;
    }

    public Duration getInterval()
    {
        return interval;
    }
}
