package com.ethlo.my2ch;

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

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.Valid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.ethlo.clackshack.ClackShack;
import com.ethlo.clackshack.ClackShackImpl;
import com.ethlo.clackshack.QueryOptions;
import com.ethlo.clackshack.model.DataTypes;
import com.ethlo.clackshack.model.QueryProgress;
import com.ethlo.clackshack.model.ResultSet;
import com.ethlo.clackshack.util.IOUtil;
import com.ethlo.my2ch.config.ClickHouseConfig;
import com.ethlo.my2ch.config.MysqlConfig;
import com.ethlo.my2ch.config.Source;
import com.ethlo.my2ch.config.Target;
import com.ethlo.my2ch.config.TransferConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class My2ch implements AutoCloseable
{
    private static final String statsQueryTemplate = IOUtil.readClasspath("stats_query.sql");
    private static final Logger logger = LoggerFactory.getLogger(My2ch.class);
    private final NamedParameterJdbcTemplate tpl;
    private final ClackShack clackShack;
    private final TransferConfig config;
    private final HikariDataSource dataSource;

    public My2ch(@Valid final TransferConfig config)
    {
        final MysqlConfig mysqlConfig = config.getSource().getMysql();
        final String mysqlUrl = mysqlConfig.getUrl();
        logger.debug("Connecting to MySQL using {}", mysqlUrl);
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(mysqlUrl);
        hikariConfig.setUsername(mysqlConfig.getUsername());
        hikariConfig.setPassword(mysqlConfig.getPassword());
        hikariConfig.setKeepaliveTime(Duration.ofMinutes(1).toMillis());
        hikariConfig.setMaximumPoolSize(1);
        hikariConfig.setLeakDetectionThreshold(Duration.ofMinutes(30).toMillis());

        this.dataSource = new HikariDataSource(hikariConfig);
        this.tpl = new NamedParameterJdbcTemplate(dataSource);
        tpl.queryForObject("SELECT 1", Collections.emptyMap(), Long.class);
        logger.debug("Connected to MySQL");

        final ClickHouseConfig chCfg = config.getTarget().getClickhouse();
        logger.debug("Connecting to ClickHouse using URL {}", chCfg.getUrl());
        this.clackShack = new ClackShackImpl(chCfg.getUrl());
        this.clackShack.query("SELECT 1");
        logger.debug("Connected to ClickHouse");

        this.config = config;
    }

    private String getClickHouseTableDefinition(final String tmpTableName, final String query, final String engineDefinition, final String clickHouseTmpDbAndTable)
    {
        tpl.update("DROP TABLE IF EXISTS " + tmpTableName, Collections.emptyMap());

        tpl.update("CREATE TEMPORARY TABLE `" + tmpTableName + "` AS " + query + " LIMIT 0", Collections.emptyMap());

        final StringBuilder s = new StringBuilder();
        s.append("CREATE TABLE ").append(clickHouseTmpDbAndTable).append(" (");
        final List<String> columns = new LinkedList<>();
        tpl.query("DESC " + tmpTableName, row ->
        {
            final String columnName = row.getString(1);
            final String mysqlType = row.getString(2);
            final String nullable = row.getString(3);
            final boolean isNullable = "YES".equalsIgnoreCase(nullable);
            final String dataType = ClickHouseTypeDefinitionConverter.fromMysqlType(mysqlType, isNullable);
            columns.add(columnName + " " + dataType);
        });
        s.append(StringUtils.collectionToDelimitedString(columns, ",\n"));
        s.append(") ");
        s.append(engineDefinition);
        return s.toString();
    }

    private ResultSet fetchStorageStats(final String databaseName, final String tableName)
    {
        final String statQuery = statsQueryTemplate
                .replace("{db}", databaseName)
                .replace("{table}", tableName);
        return clackShack.query(statQuery);
    }

    private void dropView(final String viewName)
    {
        tpl.update("DROP VIEW IF EXISTS " + viewName, Collections.emptyMap());
    }

    private String createView(final String tableName, final String query, final String rangeClause)
    {
        final String viewName = "tmp_my2ch_" + tableName;
        tpl.update("CREATE OR REPLACE VIEW " + viewName + " AS " + query + (rangeClause != null ? " " + rangeClause : ""), Collections.emptyMap());
        return viewName;
    }

    private long transferData(final String query, final String targetDb, final String targetTable, final Function<QueryProgress, Boolean> listener)
    {
        logger.debug("Transferring data from MySQL query to Clickhouse table {}.{}", targetDb, targetTable);
        final String transferQuery = "insert into " + targetDb + "." + targetTable + " " + query;
        logger.debug("Transfer query: {}", transferQuery);
        final AtomicLong max = new AtomicLong();
        clackShack.insert(transferQuery, QueryOptions.create().progressListener(queryProgress ->
        {
            logger.debug("Progress: {}", queryProgress);
            max.set(queryProgress.getReadRows());
            return listener.apply(queryProgress);
        }));
        return max.get();
    }

    private String setupView(final TransferConfig config, final boolean isIncremental, final boolean tableExists)
    {
        final Source source = config.getSource();
        final Target target = config.getTarget();
        final String targetDbAndTable = target.getClickhouse().getDb() + "." + config.getAlias();

        if (isIncremental && tableExists)
        {
            // Find current max
            final ResultSet maxResult = clackShack.query("SELECT MAX(" + target.getPrimaryKey() + ") FROM " + targetDbAndTable);
            final long max = maxResult.get(0, 0, Number.class).longValue();
            logger.debug("Current max value of column {} in Clickhouse table '{}': {}", target.getPrimaryKey(), config.getAlias(), max);

            final String rangeClauseTpl = config.getSource().getRangeClause();
            final String rangeClause = rangeClauseTpl != null ? rangeClauseTpl.replace("{max_primary_key}", "" + max) : null;
            return createView(config.getAlias(), source.getQuery(), rangeClause);
        }
        else
        {
            final String tmpTableName = "tmp_" + config.getAlias();
            final String clickHouseTmpDbAndTable = target.getClickhouse().getDb() + "." + tmpTableName;
            clackShack.ddl("DROP TABLE IF EXISTS " + clickHouseTmpDbAndTable);

            final String tableDef = getClickHouseTableDefinition(tmpTableName, source.getQuery(), target.getEngineDefinition(), clickHouseTmpDbAndTable);
            logger.debug("Clickhouse table definition: {}", tableDef);

            logger.debug("Creating clickhouse table {}", clickHouseTmpDbAndTable);
            clackShack.ddl(tableDef);

            if (clackShack.query("EXISTS TABLE " + targetDbAndTable).get(0, 0, Short.class) > 0)
            {
                // Table exists, so do an atomic exchange
                clackShack.ddl("EXCHANGE TABLES " + targetDbAndTable + " AND " + clickHouseTmpDbAndTable);
            }
            else
            {
                // It does not exist, so we simply rename it
                clackShack.ddl("RENAME TABLE " + clickHouseTmpDbAndTable + " TO " + targetDbAndTable);
            }

            return createView(config.getAlias(), source.getQuery(), null);
        }
    }

    public Map<String, Object> getStats()
    {
        final ResultSet result = fetchStorageStats(config.getTarget().getClickhouse().getDb(), config.getAlias());
        return !result.isEmpty() ? result.asMap().iterator().next() : Collections.emptyMap();
    }

    public long run(final Function<QueryProgress, Boolean> progressListener)
    {
        final Source source = config.getSource();
        final Target target = config.getTarget();
        final String qualifiedTargetTableName = config.getTarget().getClickhouse().getDb() + "." + config.getAlias();

        logger.debug("Target table is {}", qualifiedTargetTableName);
        final ResultSet result = clackShack.query("EXISTS TABLE " + qualifiedTargetTableName);
        final boolean tableExists = result.get(0, 0, Number.class).intValue() == 1;

        final boolean isIncremental = source.getRangeClause() != null;
        logger.debug("Found range-clause, so is incremental: {}", isIncremental);

        final String mysqlDbName = tpl.queryForObject("SELECT DATABASE()", Collections.emptyMap(), String.class);

        logger.debug("Connecting to source {}", config.getSource().getMysql());
        final MysqlConfig mysqlConfig = config.getSource().getMysql();

        final String createMysqlEngine = "CREATE DATABASE IF NOT EXISTS mysql_"
                + mysqlDbName + " ENGINE = MySQL('" + mysqlConfig.getHost() + ":" + mysqlConfig.getPort() + "', '" + mysqlDbName + "', '" + mysqlConfig.getUsername() + "', '" + mysqlConfig.getPassword() + "')";
        logger.debug("Command to create MySQL DB proxy in Clickhouse: {}", createMysqlEngine);

        clackShack.ddl(createMysqlEngine);
        logger.debug("MySQL database connection created from ClickHouse to MySQL");

        final String viewName = setupView(config, isIncremental, tableExists);

        logger.debug("Starting transfer from MySQL view {} to ClickHouse table {}", viewName, config.getAlias());
        final long transferred = transferData("SELECT * FROM mysql_" + mysqlDbName + "." + viewName, target.getClickhouse().getDb(), config.getAlias(), progressListener);

        logger.debug("Dropping view {} in MySQL", viewName);
        dropView(viewName);

        return transferred;
    }

    public TransferConfig getConfig()
    {
        return config;
    }

    @Override
    public void close()
    {
        this.dataSource.close();
    }
}
