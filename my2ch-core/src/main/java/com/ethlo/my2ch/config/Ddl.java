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
