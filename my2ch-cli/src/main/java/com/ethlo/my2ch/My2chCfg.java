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

import com.ethlo.my2ch.scheduler.My2chTaskRunner;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class My2chCfg
{
    @Bean
    public My2chTaskRunner scheduler()
    {
        return new My2chTaskRunner(1);
    }
}
