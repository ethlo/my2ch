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
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;

@Valid
public class Target
{
    @Valid
    @NotNull
    private final ClickHouseConfig clickhouse;

    @NotNull
    private final String primaryKey;

    @NotNull
    private final String engineDefinition;

    public Target(@JsonProperty("clickhouse_config") final ClickHouseConfig clickhouse,
                  @JsonProperty("primary_key") final String primaryKey,
                  @JsonProperty("engine_definition") final String engineDefinition)
    {
        this.clickhouse = clickhouse;
        this.primaryKey = primaryKey;
        this.engineDefinition = engineDefinition;
    }

    public String getPrimaryKey()
    {
        return primaryKey;
    }

    public String getEngineDefinition()
    {
        return engineDefinition;
    }

    public ClickHouseConfig getClickhouse()
    {
        return clickhouse;
    }
}
