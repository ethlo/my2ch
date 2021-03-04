package com.ethlo.my2ch;

public class MysqlConfig
{
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final String dbName;

    public MysqlConfig(final String host, final int port, final String username, final String password, final String dbName)
    {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.dbName = dbName;
    }

    public String getHost()
    {
        return host;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public int getPort()
    {
        return port;
    }

    public String getDbName()
    {
        return dbName;
    }
}
