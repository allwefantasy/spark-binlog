package org.apache.spark.sql.mlsql.sources.mysql.binlog;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.spark.sql.catalyst.json.JSONOptions;
import org.apache.spark.sql.mlsql.sources.mysql.binlog.io.SchemaTool;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Locale;
import java.util.TimeZone;

/**
 * 2019-06-13 WilliamZhu(allwefantasy@gmail.com)
 */
public class MySQLCDCUtils {


    public static Object getWritableObject(SchemaTool schemaTool, Serializable value) {
//        if(true){
//            throw new RuntimeException("----");
//        }
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return new String((byte[]) value, Charset.forName(schemaTool.charset()));
        }

        if (value instanceof BitSet) {
            BitSet bsValue = ((BitSet) value);
            if (schemaTool.isBoolean()) {
                return bsValue.get(0) ? true : false;
            }

            if (schemaTool.isLong()) {
                long longValue = 0;
                byte[] by = bsValue.toByteArray();
                for (int i = 0; i < by.length; i++)
                {
                    longValue += ((long) by[i] & 0xffL) << (8 * i);
                }
                return longValue;
            }
            throw new RuntimeException("cannot serializer for " + schemaTool.field().name());
        }

        if (schemaTool.isTimestamp() && value instanceof java.sql.Timestamp) {
            java.sql.Timestamp item = (java.sql.Timestamp) value;
            JSONOptions options = JsonOptions.options(schemaTool.timeZone());
            FastDateFormat timestampFormat = FastDateFormat.getInstance(options.timestampFormat(),
                TimeZone.getTimeZone(options.zoneId()), options.locale());
            return timestampFormat.format(item);
        }

        // mysql datetime type
        if (schemaTool.isTimestamp() && value instanceof java.util.Date) {
            org.joda.time.DateTime item = new org.joda.time.DateTime((java.util.Date) value);
            int offset = TimeZone.getTimeZone(ZoneId.of(schemaTool.timeZone())).getRawOffset();
            JSONOptions options = JsonOptions.options(schemaTool.timeZone());
            FastDateFormat timestampFormat = FastDateFormat.getInstance(options.timestampFormat(),
                TimeZone.getTimeZone(options.zoneId()), options.locale());
            return timestampFormat.format(item.minusMillis(offset).toDate());
        }

        if (schemaTool.isDate() && value instanceof java.sql.Date) {
            java.sql.Date item = (java.sql.Date) value;
            JSONOptions options = JsonOptions.options(schemaTool.timeZone());
            FastDateFormat dateFormat = FastDateFormat.getInstance(options.dateFormat(), options.locale());
            return dateFormat.format(item);
        }

        if (schemaTool.isDate() && value instanceof java.util.Date) {
            java.util.Date item = (java.util.Date) value;
            JSONOptions options = JsonOptions.options(schemaTool.timeZone());
            FastDateFormat dateFormat = FastDateFormat.getInstance(options.dateFormat(), options.locale());
            return dateFormat.format(item);
        }

        return value;
    }
}

class BitSetSerializer extends JsonSerializer<BitSet> {

    public void serialize(BitSet value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {
        gen.writeStartArray();
        for (long l : value.toLongArray()) {
            gen.writeNumber(l);
        }
        gen.writeEndArray();
    }
}

class BitSetDeserializer extends JsonDeserializer<BitSet> {

    @Override
    public BitSet deserialize(JsonParser jsonParser, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ArrayList<Long> l = new ArrayList<Long>();
        JsonToken token;
        while (!JsonToken.END_ARRAY.equals(token = jsonParser.nextValue())) {
            if (token.isNumeric()) {
                l.add(jsonParser.getLongValue());
            }
        }

        long[] array = new long[l.size()];
        for (int i = 0; i < array.length; i++) {
            array[i] = l.get(i).longValue();
        }
        return BitSet.valueOf(array);
    }
}
