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

import java.net.URI;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@Valid
public class MysqlConfig
{
    @NotNull
    private final String url;

    private final String username;
    private final String password;
    private final int port;
    private final String host;

    public MysqlConfig(final String url)
    {
        this.url = url;

        final URI uri = URI.create(URI.create(url).getSchemeSpecificPart());
        this.host = uri.getHost();

        if (uri.getUserInfo() != null)
        {
            final String[] userInfo = uri.getUserInfo().split(":");
            this.username = userInfo[0];
            this.password = userInfo[1];
        }
        else
        {
            this.username = null;
            this.password = null;
        }

        this.port = uri.getPort() > 0 ? uri.getPort() : 3306;
    }

    public MysqlConfig(final String url, final String username, final String password)
    {
        this.url = url;
        this.username = username;
        this.password = password;
        final URI uri = URI.create(URI.create(url).getSchemeSpecificPart());
        this.host = uri.getHost();
        this.port = uri.getPort() > 0 ? uri.getPort() : 3306;
    }

    public String getUrl()
    {
        return url;
    }

    @Override
    public String toString()
    {
        return "MysqlConfig {url=" + url + "}";
    }

    public int getPort()
    {
        return port;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public String getHost()
    {
        return host;
    }
}
