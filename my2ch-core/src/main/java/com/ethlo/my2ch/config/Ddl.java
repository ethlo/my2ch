package com.ethlo.my2ch.config;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Valid
public class Ddl
{
    @NotEmpty
    private final String version;

    @NotEmpty
    private final String query;

    @NotNull
    private final LifeCycle lifecycle;

    public Ddl(final String version, final String query, final LifeCycle lifecycle)
    {
        this.version = version;
        this.query = query;
        this.lifecycle = lifecycle;
    }

    public String getVersion()
    {
        return version;
    }

    public String getQuery()
    {
        return query;
    }

    public LifeCycle getLifecycle()
    {
        return lifecycle;
    }

    @Override
    public String toString()
    {
        return "Ddl {" +
                "version='" + version + '\'' +
                ", lifecycle=" + lifecycle +
                '}';
    }
}
