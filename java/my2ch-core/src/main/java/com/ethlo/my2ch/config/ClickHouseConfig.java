package com.ethlo.my2ch.config;

import javax.validation.constraints.NotNull;

public class ClickHouseConfig
{
    @NotNull
    private final String db;

    @NotNull
    private final String url;

    public ClickHouseConfig(final @NotNull String db, final @NotNull String url)
    {
        this.db = db;
        this.url = url;
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
