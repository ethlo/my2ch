package com.ethlo.my2ch.config;

import javax.validation.constraints.NotNull;

public class ClickHouseConfig
{
    @NotNull
    private final String url;

    @NotNull
    private final String db;

    public ClickHouseConfig(final String url, final String db)
    {
        this.url = url;
        this.db = db;
    }

    public String getUrl()
    {
        return url;
    }

    public String getDb()
    {
        return db;
    }
}
