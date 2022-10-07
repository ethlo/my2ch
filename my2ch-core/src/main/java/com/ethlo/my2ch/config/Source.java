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
public class Source
{
    @Valid
    @NotNull
    private final MysqlConfig mysql;

    @NotNull
    private final String query;

    private final String rangeClause;

    public Source(
            @JsonProperty("mysql") final MysqlConfig mysql,
            @JsonProperty("query") final String query,
            @JsonProperty("range-clause") final String rangeClause)
    {
        this.mysql = mysql;
        this.rangeClause = rangeClause;
        this.query = query;
    }

    public String getQuery()
    {
        return query;
    }

    public String getRangeClause()
    {
        return rangeClause;
    }

    public MysqlConfig getMysql()
    {
        return mysql;
    }
}
