package com.ethlo.my2ch.actuator;

/*-
 * #%L
 * Simple MySQL to ClickHouse ETL - CLI
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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.ethlo.my2ch.scheduler.My2chTaskRunner;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

import com.ethlo.clackshack.ClackShack;
import com.ethlo.clackshack.ClackShackImpl;
import com.ethlo.clackshack.model.ResultSet;
import com.ethlo.my2ch.config.ClickHouseConfig;
import com.ethlo.my2ch.config.TransferConfig;

@Component
@Endpoint(id = "clickhouse")
public class ClickhouseTablesEndpoint
{
    private final My2chTaskRunner taskRunner;

    public ClickhouseTablesEndpoint(final My2chTaskRunner taskRunner)
    {
        this.taskRunner = taskRunner;
    }

    @ReadOperation()
    public List<Map<String, Object>> size()
    {
        final List<Map<String, Object>> results = new LinkedList<>();
        final Map<String, TransferConfig> tasks = taskRunner.getTasks();
        for (final Map.Entry<String, TransferConfig> entry : tasks.entrySet())
        {
            final String name = entry.getKey();
            final TransferConfig task = entry.getValue();
            final ClickHouseConfig clickhouseCfg = task.getTarget().getClickhouse();
            final ClackShack clackShack = new ClackShackImpl(clickhouseCfg.getUrl());

            final Map<String, Object> params = new LinkedHashMap<>();
            params.put("db", clickhouseCfg.getDb());
            params.put("table", name);

            final ResultSet result = clackShack.query(
                    "SELECT bytes\n" +
                            "FROM system.parts\n" +
                            "WHERE database = :db " +
                            "AND table = :table", params);
            final long size = result.isEmpty() ? 0 : result
                    .getRow(0)
                    .get("bytes", Number.class)
                    .longValue();

            results.add(Collections.singletonMap(name, Collections.singletonMap("size", size)));
        }
        return results;
    }
}
