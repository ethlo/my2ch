package com.ethlo.my2ch;

import java.time.Duration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TransferConfig
{
    private final String alias;
    private final Schedule schedule;
    private final Source source;
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

    public static class Source
    {
        private final String query;
        private final String rangeClause;

        public Source(@JsonProperty("query") final String query,
                      @JsonProperty("range_clause") final String rangeClause)
        {
            this.rangeClause = rangeClause;
            this.query = query;
        }

        public String getQuery()
        {
            return query;
        }

        public String getRangeClause()
        {
            return rangeClause;
        }
    }

    public static class Target
    {
        private final String dbName;
        private final String primaryKey;
        private final String engineDefinition;

        public Target(@JsonProperty("db") final String db,
                      @JsonProperty("primary_key") final String primaryKey,
                      @JsonProperty("engine_definition") final String engineDefinition)
        {
            this.dbName = db;
            this.primaryKey = primaryKey;
            this.engineDefinition = engineDefinition;
        }

        public String getDb()
        {
            return dbName != null ? dbName : "default";
        }

        public String getPrimaryKey()
        {
            return primaryKey;
        }

        public String getEngineDefinition()
        {
            return engineDefinition;
        }
    }

    public static class Schedule
    {
        private final Duration interval;

        public Schedule(@JsonProperty("interval") final Duration interval)
        {
            this.interval = interval;
        }

        public Duration getInterval()
        {
            return interval;
        }
    }
}

