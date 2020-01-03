package org.apache.spark.sql.mlsql.sources.mysql.binlog;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.spark.sql.mlsql.sources.mysql.binlog.io.SchemaTool;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.time.ZoneId;
import java.util.TimeZone;

/**
 * 2019-06-13 WilliamZhu(allwefantasy@gmail.com)
 */
public class MySQLCDCUtils {


    public static Object getWritableObject(SchemaTool schemaTool, Serializable value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return new String((byte[]) value, Charset.forName(schemaTool.charset()));
        }
        if (schemaTool.isTimestamp() && value instanceof java.sql.Timestamp) {
            java.sql.Timestamp item = (java.sql.Timestamp) value;
            FastDateFormat timestampFormat = JsonOptions.options(schemaTool.timeZone()).timestampFormat();
            return timestampFormat.format(item);
        }

        // mysql datetime type
        if (schemaTool.isTimestamp() && value instanceof java.util.Date) {
            org.joda.time.DateTime item = new org.joda.time.DateTime((java.util.Date) value);
            int offset = TimeZone.getTimeZone(ZoneId.of(schemaTool.timeZone())).getRawOffset();
            FastDateFormat timestampFormat = JsonOptions.options(schemaTool.timeZone()).timestampFormat();
            return timestampFormat.format(item.minusMillis(offset).toDate());
        }

        if (schemaTool.isDate() && value instanceof java.sql.Date) {
            java.sql.Date item = (java.sql.Date) value;
            FastDateFormat dateFormat = JsonOptions.options(schemaTool.timeZone()).dateFormat();
            return dateFormat.format(item);
        }

        if (schemaTool.isDate() && value instanceof java.util.Date) {
            java.util.Date item = (java.util.Date) value;
            FastDateFormat dateFormat = JsonOptions.options(schemaTool.timeZone()).dateFormat();
            return dateFormat.format(item);
        }

        return value;
    }
}
