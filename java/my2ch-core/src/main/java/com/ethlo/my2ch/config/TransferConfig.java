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

import com.fasterxml.jackson.annotation.JsonProperty;

public class TransferConfig
{
    @NotNull
    private final String alias;

    @Valid
    @NotNull
    private final Schedule schedule;

    @Valid
    @NotNull
    private final Source source;

    @Valid
    @NotNull
    private final Target target;

    public TransferConfig(@JsonProperty("alias") final String alias,
                          @JsonProperty("schedule") final Schedule schedule,
                          @JsonProperty("source") final Source source,
                          @JsonProperty("target") final Target target)
    {
        this.alias = alias;
        this.schedule = schedule;
        this.source = source;
        this.target = target;
    }

    public String getAlias()
    {
        return alias;
    }

    public Schedule getSchedule()
    {
        return schedule;
    }

    public Source getSource()
    {
        return source;
    }

    public Target getTarget()
    {
        return target;
    }

}

