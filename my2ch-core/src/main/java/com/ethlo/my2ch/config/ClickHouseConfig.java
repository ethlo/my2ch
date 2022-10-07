package com.ethlo.my2ch.config;

/*-
 * #%L
 * Simple MySQL to ClickHouse ETL - Core
 * %%
 * Copyright (C) 2018 - 2022 Morten Haraldsen (ethlo)
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
