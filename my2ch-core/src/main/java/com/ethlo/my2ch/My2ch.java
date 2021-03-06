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
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
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

public class My2ch implements AutoCloseable
{
    private static final String statsQueryTemplate = IOUtil.readClasspath("stats_query.sql");
    private static final Logger logger = LoggerFactory.getLogger(My2ch.class);
    private final NamedParameterJdbcTemplate tpl;
    private final ClackShack clackShack;
    private final TransferConfig config;
    private final SingleConnectionDataSource dataSource;

    public My2ch(@Valid final TransferConfig config)
    {
        final MysqlConfig mysqlConfig = config.getSource().getMysql();
        final String url = "jdbc:mysql://" + mysqlConfig.getHost() + ":" + mysqlConfig.getPort() + "/" + mysqlConfig.getDb() + "?useUnicode=yes&characterEncoding=UTF-8&rewriteBatchedStatements=true";
        logger.info("Connecting to {}", url);
        this.dataSource = new SingleConnectionDataSource(url, mysqlConfig.getUsername(), mysqlConfig.getPassword(), true);
        this.tpl = new NamedParameterJdbcTemplate(dataSource);
        tpl.queryForObject("SELECT 1", Collections.emptyMap(), Long.class);
        final ClickHouseConfig chCfg = config.getTarget().getClickhouse();
        this.clackShack = new ClackShackImpl("http://" + chCfg.getHost() + ":" + chCfg.getPort());
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
            final String tableDef = getClickHouseTableDefinition(source.getQuery(), target.getEngineDefinition(), target.getClickhouse().getDb(), config.getAlias());
            logger.debug("Clickhouse table definition: {}", tableDef);

            clackShack.ddl("DROP TABLE IF EXISTS " + targetDbAndTable);
            logger.debug("Creating clickhouse table {}", config.getAlias());
            clackShack.ddl(tableDef).join();
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

        final MysqlConfig mysqlConfig = config.getSource().getMysql();

        final String createMysqlEngine = "CREATE DATABASE IF NOT EXISTS mysql_"
                + mysqlDbName + " ENGINE = MySQL('" + mysqlConfig.getHost() + "', '" + mysqlDbName + "', '" + mysqlConfig.getUsername() + "', '" + mysqlConfig.getPassword() + "')";
        logger.debug("Command to create MySQL DB proxy in Clickhouse: {}", createMysqlEngine);
        clackShack.ddl(createMysqlEngine).join();

        final String viewName = setupView(config, isIncremental, tableExists);

        final long transferred = transferData("SELECT * FROM mysql_" + mysqlDbName + "." + viewName, target.getClickhouse().getDb(), config.getAlias(), progressListener);
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
        this.dataSource.destroy();
    }
}
