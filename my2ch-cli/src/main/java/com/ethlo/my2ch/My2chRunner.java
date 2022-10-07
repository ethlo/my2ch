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

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import picocli.CommandLine;

@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class My2chRunner implements CommandLineRunner, ExitCodeGenerator
{
    private final TransferCommand transferCommand;

    private int exitCode;

    public My2chRunner(final TransferCommand transferCommand)
    {
        this.transferCommand = transferCommand;
    }

    public static void main(String[] args)
    {
        // let Spring instantiate and inject dependencies
        System.exit(SpringApplication.exit(SpringApplication.run(My2chRunner.class, args)));
    }

    @Override
    public void run(String... args)
    {
        exitCode = new CommandLine(transferCommand).execute(args);
    }

    @Override
    public int getExitCode()
    {
        return exitCode;
    }
}
