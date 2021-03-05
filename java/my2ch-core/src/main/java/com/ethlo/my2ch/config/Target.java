package com.ethlo.my2ch.config;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;

@Valid
public class Target
{
    @Valid
    @NotNull
    private final ClickHouseConfig clickhouse;

    @NotNull
    private final String primaryKey;

    @NotNull
    private final String engineDefinition;

    public Target(@JsonProperty("clickhouse_config") final ClickHouseConfig clickhouse,
                  @JsonProperty("primary_key") final String primaryKey,
                  @JsonProperty("engine_definition") final String engineDefinition)
    {
        this.clickhouse = clickhouse;
        this.primaryKey = primaryKey;
        this.engineDefinition = engineDefinition;
    }

    public String getPrimaryKey()
    {
        return primaryKey;
    }

    public String getEngineDefinition()
    {
        return engineDefinition;
    }

    public ClickHouseConfig getClickhouse()
    {
        return clickhouse;
    }
}
