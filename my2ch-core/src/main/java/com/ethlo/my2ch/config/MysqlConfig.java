package com.ethlo.my2ch.config;

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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Valid
public class MysqlConfig
{
    @NotNull
    private final String host;

    @NotNull
    private final int port;

    @NotNull
    private final String username;

    @NotNull
    private final String password;

    @NotNull
    private final String db;

    public MysqlConfig(final String host, final int port, final String username, final String password, final String db)
    {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.db = db;
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

    public String getDb()
    {
        return db;
    }

    @Override
    public String toString()
    {
        return "MysqlConfig{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", password='" + redactPassword(password) + '\'' +
                ", db='" + db + '\'' +
                '}';
    }

    private static String redactPassword(final String password)
    {
        if (password == null)
        {
            return null;
        }
        else if (password.length() <= 2)
        {
            return "*****";
        }
        return password.charAt(0) + "******" + password.charAt(password.length() - 1);
    }
}
