package com.ethlo.my2ch;

/*-
 * #%L
 * my2ch
 * %%
 * Copyright (C) 2021 Morten Haraldsen (ethlo)
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

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
