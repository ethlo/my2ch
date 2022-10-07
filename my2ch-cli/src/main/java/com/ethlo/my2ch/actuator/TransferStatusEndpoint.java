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

import java.util.List;
import java.util.Map;

import com.ethlo.my2ch.scheduler.My2chTaskRunner;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.stereotype.Component;

@Component
@Endpoint(id = "transfers")
public class TransferStatusEndpoint
{
    private final My2chTaskRunner scheduler;

    public TransferStatusEndpoint(final My2chTaskRunner scheduler)
    {
        this.scheduler = scheduler;
    }

    @ReadOperation
    public List<Map<String, Object>> status()
    {
        return scheduler.getTaskStatuses();
    }
}
