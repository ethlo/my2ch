package com.ethlo.my2ch;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.util.StringUtils;

import com.ethlo.clackshack.ClackShack;
import com.ethlo.clackshack.QueryOptions;
import com.ethlo.clackshack.model.DataTypes;
import com.ethlo.clackshack.model.ResultSet;
import com.ethlo.clackshack.util.IOUtil;

public class My2ch
{
    private static final String statsQueryTemplate = IOUtil.readClasspath("stats_query.sql");
    private static final Logger logger = LoggerFactory.getLogger(My2ch.class);
    private final NamedParameterJdbcTemplate tpl;
    private final ClackShack clackShack;
    private final MysqlConfig mysqlConfig;

    public My2ch(final MysqlConfig mysqlConfig, final ClackShack clackShack)
    {
        this.mysqlConfig = mysqlConfig;
        final String url = "jdbc:mysql://" + mysqlConfig.getHost() + ":" + mysqlConfig.getPort() + "/" + mysqlConfig.getDbName() + "?useUnicode=yes&characterEncoding=UTF-8&rewriteBatchedStatements=true";
        this.tpl = new NamedParameterJdbcTemplate(new SingleConnectionDataSource(url, mysqlConfig.getUsername(), mysqlConfig.getPassword(), true));
        tpl.queryForObject("SELECT 1", Collections.emptyMap(), Long.class);
        this.clackShack = clackShack;
    }

    public String convertMysqlToClickhouseType(final String mysqlType)
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

    private String prependUnsigned(final boolean isUnsigned, final String type)
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

    public void processSingle(TransferConfig config)
    {
        final TransferConfig.Source source = config.getSource();
        final TransferConfig.Target target = config.getTarget();
        final boolean isIncremental = source.getRangeClause() != null;
        final String tableDef = getClickHouseTableDefinition(source.getQuery(), target.getEngineDefinition(), target.getDb(), config.getAlias());
        logger.debug("Clickhouse table definition: {}", tableDef);
        final ResultSet result = clackShack.query("EXISTS TABLE " + config.getAlias()).join();
        final boolean tableExists = result.get(0, 0, Number.class).intValue() == 1;
        final String mysqlDbName = tpl.queryForObject("SELECT DATABASE()", Collections.emptyMap(), String.class);

        final String createMysqlEngine = "CREATE DATABASE IF NOT EXISTS mysql_"
                + mysqlDbName + " ENGINE = MySQL(" + mysqlConfig.getHost() + ", '" + mysqlDbName + "', '" + mysqlConfig.getUsername() + "', '" + mysqlConfig.getPassword() + "')";
        logger.debug("Command to create MySQL DB proxy in Clickhouse: {}", createMysqlEngine);
        clackShack.ddl(createMysqlEngine).join();

        final String viewName = setupView(config, isIncremental, tableExists, tableDef);

        transferData("SELECT * FROM mysql_" + mysqlDbName + "." + viewName, target.getDb(), config.getAlias());
        dropView(viewName);
    }

    private void transferData(final String query, final String targetDb, final String targetTable)
    {
        logger.debug("Transferring data from MySQL query to Clickhouse table {}.{}", targetDb, targetTable);
        final String transferQuery = "insert into " + targetDb + "." + targetTable + " " + query;
        logger.debug("Transfer query: {}", transferQuery);
        clackShack.insert(transferQuery, QueryOptions.create().progressListener(queryProgress ->
        {
            logger.info("Progress: {}", queryProgress);
            return true;
        })).join();
    }

    private String setupView(final TransferConfig config, final boolean isIncremental, final boolean tableExists, final String tableDef)
    {
        final TransferConfig.Source source = config.getSource();
        final TransferConfig.Target target = config.getTarget();
        final String targetDbAndTable = target.getDb() + "." + config.getAlias();

        if (isIncremental && tableExists)
        {
            // Find current max
            final ResultSet maxResult = clackShack.query("SELECT MAX(" + target.getPrimaryKey() + ") FROM " + targetDbAndTable).join();
            final long max = maxResult.get(0, 0, Number.class).longValue();
            logger.info("Current max value of column {} in Clickhouse table '{}': {}", target.getPrimaryKey(), config.getAlias(), max);

            final String rangeClauseTpl = config.getSource().getRangeClause();
            final String rangeClause = rangeClauseTpl != null ? rangeClauseTpl.replaceAll("\\$\\{max_primary_key}", "" + max) : null;
            return createView(config.getAlias(), source.getQuery(), rangeClause);
        }
        else
        {
            clackShack.ddl("DROP TABLE IF EXISTS " + targetDbAndTable);
            logger.debug("Creating clickhouse table {}", config.getAlias());
            clackShack.ddl(tableDef).join();
            return createView(config.getAlias(), source.getQuery(), null);
        }
    }

    public Map<String, Object> getStats(final TransferConfig config)
    {
        return fetchStorageStats(config.getTarget().getDb(), config.getAlias()).asMap().iterator().next();
    }
}
