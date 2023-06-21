package com.ethlo.my2ch;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.util.Assert;

import com.ethlo.clackshack.model.DataTypes;

public class ClickHouseTypeDefinitionConverter
{
    private  ClickHouseTypeDefinitionConverter()
    {

    }

    public static String fromMysqlType(final String mysqlType, final boolean isNullable)
    {
        return nullable(fromMysqlType(mysqlType), isNullable);
    }

    public static String fromMysqlType(final String mysqlType)
    {
        final String lower = mysqlType.toLowerCase();
        final boolean isUnsigned = lower.contains("unsigned");
        if ("tinyint".equals(lower))
        {
            return prependUnsigned(false, DataTypes.INT_8.getName());
        }
        else if ("smallint".equals(lower))
        {
            return prependUnsigned(false, DataTypes.INT_16.getName());
        }
        else if ("mediumint".equals(lower))
        {
            return prependUnsigned(false, DataTypes.INT_32.getName());
        }
        else if (lower.contains("bigint"))
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
            final Pattern pattern = Pattern.compile("([\\d]+)");
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

    private static String nullable(final String param, final boolean isNullable)
    {
        return isNullable ? "Nullable(" + param + ")" : param;
    }
}
