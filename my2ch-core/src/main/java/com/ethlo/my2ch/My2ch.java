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
        this.clackShack.query("SELECT 1").join();
        logger.debug("Connected to ClickHouse");

        this.config = config;
    }

    public static String convertMysqlToClickhouseType(final String mysqlType)
    {
        final String lower = mysqlType.toLowerCase();
        final boolean isUnsigned = lower.contains("unsigned");
        switch (lower)
        {
            case "tinyint":
                return prependUnsigned(isUnsigned, DataTypes.INT_8.getName());
            case "smallint":
                return prependUnsigned(isUnsigned, DataTypes.INT_16.getName());
            case "mediumint":
                return prependUnsigned(isUnsigned, DataTypes.INT_32.getName());
        }

        if (lower.contains("bigint"))
        {
            return prependUnsigned(isUnsigned, DataTypes.INT_64.getName());
        }
        else if (lower.contains("int"))
        {
            return prependUnsigned(isUnsigned, DataTypes.INT_32.getName());
        }
        else if (lower.contains("float"))
        {
            return prependUnsigned(isUnsigned, DataTypes.FLOAT_32.getName());
        }
        else if (lower.contains("double"))
        {
            return prependUnsigned(isUnsigned, DataTypes.FLOAT_64.getName());
        }
        else if (lower.contains("datetime") || lower.contains("timestamp"))
        {
            return prependUnsigned(isUnsigned, DataTypes.DATE_TIME.getName());
        }
        else if (lower.contains("decimal"))
        {
            final Pattern pattern = Pattern.compile("([0-9]+)");
            final Matcher matcher = pattern.matcher(lower);
            Assert.isTrue(matcher.find(), "Should have number of digits: " + lower);
            final int p = Integer.parseInt(matcher.group(1));
            Assert.isTrue(matcher.find(), "Should have numbers of fraction digits: " + lower);
            final int s = Integer.parseInt(matcher.group(1));
            return DataTypes.DECIMAL.getName() + "(" + p + "," + s + ")";
        }
        else if (lower.contains("date"))
        {
            return prependUnsigned(isUnsigned, DataTypes.DATE.getName());
        }
        else if (lower.contains("bit") || lower.contains("boolean"))
        {
            return prependUnsigned(isUnsigned, DataTypes.UINT_8.getName());
        }

        return DataTypes.STRING.getName();
    }

    private static String prependUnsigned(final boolean isUnsigned, final String type)
    {
        return isUnsigned ? "U" + type : type;
    }

    private String nullable(final String param, final boolean isNullable)
    {
        return isNullable ? "Nullable(" + param + ")" : param;
    }

    private String getClickHouseTableDefinition(final String query, final String engineDefinition, final String clickHouseTargetDb, final String clickHouseTargetTableName)
    {
        final String tmpName = "my2ch_" + clickHouseTargetTableName;

        tpl.update("DROP TABLE IF EXISTS " + tmpName, Collections.emptyMap());

        tpl.update("CREATE TEMPORARY TABLE `" + tmpName + "` AS " + query + " LIMIT 0", Collections.emptyMap());

        final StringBuilder s = new StringBuilder();
        s.append("CREATE TABLE ").append(clickHouseTargetDb).append(".").append(clickHouseTargetTableName).append(" (");
        final List<String> columns = new LinkedList<>();
        tpl.query("DESC " + tmpName, row ->
        {
            final String columnName = row.getString(1);
            final String mysqlType = row.getString(2);
            final String nullable = row.getString(3);
            final boolean isNullable = "YES".equals(nullable);
            final String dataType = nullable(convertMysqlToClickhouseType(mysqlType), isNullable);
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
                .replaceAll("\\{db}", databaseName)
                .replaceAll("\\{table}", tableName);
        return clackShack.query(statQuery).join();
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
        })).join();
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
            final ResultSet maxResult = clackShack.query("SELECT MAX(" + target.getPrimaryKey() + ") FROM " + targetDbAndTable).join();
            final long max = maxResult.get(0, 0, Number.class).longValue();
            logger.debug("Current max value of column {} in Clickhouse table '{}': {}", target.getPrimaryKey(), config.getAlias(), max);

            final String rangeClauseTpl = config.getSource().getRangeClause();
            final String rangeClause = rangeClauseTpl != null ? rangeClauseTpl.replaceAll("\\{max_primary_key}", "" + max) : null;
            return createView(config.getAlias(), source.getQuery(), rangeClause);
        }
        else
        {
            final String tmpTableName = "tmp_" + config.getAlias() + "_" + System.currentTimeMillis();
            final String tmpDbAndTable = target.getClickhouse().getDb() + "." + tmpTableName;
            final String tableDef = getClickHouseTableDefinition(source.getQuery(), target.getEngineDefinition(), target.getClickhouse().getDb(), tmpDbAndTable);
            logger.debug("Clickhouse table definition: {}", tableDef);

            logger.debug("Creating clickhouse table {}", config.getAlias());
            clackShack.ddl(tableDef).join();

            clackShack.ddl("EXCHANGE TABLES " + targetDbAndTable + " AND " + tmpDbAndTable);

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
        final boolean isIncremental = source.getRangeClause() != null;
        final ResultSet result = clackShack.query("EXISTS TABLE " + config.getAlias()).join();
        final boolean tableExists = result.get(0, 0, Number.class).intValue() == 1;

        final String mysqlDbName = tpl.queryForObject("SELECT DATABASE()", Collections.emptyMap(), String.class);

        logger.debug("Connecting to source {}", config.getSource().getMysql());
        final MysqlConfig mysqlConfig = config.getSource().getMysql();

        final String createMysqlEngine = "CREATE DATABASE IF NOT EXISTS mysql_"
                + mysqlDbName + " ENGINE = MySQL('" + mysqlConfig.getHost() + ":" + mysqlConfig.getPort() + "', '" + mysqlDbName + "', '" + mysqlConfig.getUsername() + "', '" + mysqlConfig.getPassword() + "')";
        logger.debug("Command to create MySQL DB proxy in Clickhouse: {}", createMysqlEngine);

        clackShack.ddl(createMysqlEngine).join();
        logger.debug("MySQL database connection created from ClickHouse to MySQL");

        final String viewName = setupView(config, isIncremental, tableExists);

        logger.debug("Starting transfer from MySQL view {} to ClickHouse table {}", viewName, config.getAlias());
        final long transferred = transferData("SELECT * FROM mysql_" + mysqlDbName + "." + viewName, target.getClickhouse().getDb(), config.getAlias(), progressListener);

        logger.debug("Dropping view {} in MySQL", viewName);
        dropView(viewName);

        // logger.debug("Dropping MySQL database engine created from ClickHouse to MySQL");
        //clackShack.ddl("DROP DATABASE IF EXISTS mysql_" + mysqlDbName).join();

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
