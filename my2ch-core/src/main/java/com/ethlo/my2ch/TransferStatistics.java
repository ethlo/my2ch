package com.ethlo.my2ch;

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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Map;

public record TransferStatistics(long rows, OffsetDateTime started, Duration elapsed,
                                 Map<String, Object> tableStatistics)
{
    @JsonProperty("rows_per_second")
    public double getRowsPerSecond()
    {
        return (rows * 1_000_000_000D) / elapsed.toNanos();
    }

    @JsonProperty("table_stats")
    public Map<String, Object> getTableStatistics()
    {
        return tableStatistics;
    }
}
