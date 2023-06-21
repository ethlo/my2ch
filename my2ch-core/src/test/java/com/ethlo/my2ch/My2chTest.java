package com.ethlo.my2ch;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class My2chTest
{
    @Test
    void testConvertMysqlToClickhouseType()
    {
        final String type = ClickHouseTypeDefinitionConverter.fromMysqlType("decimal (3, 6 )");
        assertThat(type).isEqualTo("Decimal(3,6)");
    }
}