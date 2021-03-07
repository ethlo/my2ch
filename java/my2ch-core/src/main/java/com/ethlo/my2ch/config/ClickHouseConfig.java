package com.ethlo.my2ch.config;

import javax.validation.constraints.NotNull;

public class ClickHouseConfig
{
    @NotNull
    private final String db;

    @NotNull
    private final String host;

    @NotNull
    private final int port;

    public ClickHouseConfig(final @NotNull String db, final @NotNull String host, final int port)
    {
        this.db = db;
        this.host = host;
        this.port = port;
    }

    public String getDb()
    {
        return db;
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }
}
