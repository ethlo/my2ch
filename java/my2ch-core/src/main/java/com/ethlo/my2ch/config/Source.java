package com.ethlo.my2ch.config;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;

@Valid
public class Source
{
    @Valid
    @NotNull
    private final MysqlConfig mysql;

    @NotNull
    private final String query;

    private final String rangeClause;

    public Source(
            @JsonProperty("mysql") final MysqlConfig mysql,
            @JsonProperty("query") final String query,
            @JsonProperty("range_clause") final String rangeClause)
    {
        this.mysql = mysql;
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

    public MysqlConfig getMysql()
    {
        return mysql;
    }
}
