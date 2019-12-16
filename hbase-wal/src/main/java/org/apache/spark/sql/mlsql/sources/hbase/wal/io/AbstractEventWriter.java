package org.apache.spark.sql.mlsql.sources.hbase.wal.io;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.spark.sql.mlsql.sources.hbase.wal.RawHBaseWALEvent;

import java.io.IOException;
import java.io.StringWriter;

/**
 * 12/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
public abstract class AbstractEventWriter {
    private final JsonFactory JSON_FACTORY = new JsonFactory();
    protected JsonGenerator jsonGenerator;

    protected void startJson(StringWriter outputStream, RawHBaseWALEvent event) throws IOException {
        jsonGenerator = createJsonGenerator(outputStream);
        jsonGenerator.writeStartObject();
        String eventType = event.put() != null ? "put" : "delete";
        if (eventType == null) {
            jsonGenerator.writeNullField("type");
        } else {
            jsonGenerator.writeStringField("type", eventType);
        }
        Long timestamp = event.time();
        if (timestamp == null) {
            jsonGenerator.writeNullField("timestamp");
        } else {
            jsonGenerator.writeNumberField("timestamp", timestamp);
        }

        if (event.db() != null) {
            String db = event.db();
            String tableName = event.table();

            jsonGenerator.writeStringField("databaseName", db);
            jsonGenerator.writeStringField("tableName", tableName);
            jsonGenerator.writeNullField("schema");
        } else {
            jsonGenerator.writeNullField("databaseName");
            jsonGenerator.writeNullField("tableName");
            jsonGenerator.writeNullField("schema");
        }


    }

    protected void endJson() throws IOException {
        if (jsonGenerator == null) {
            throw new IOException("endJson called without a JsonGenerator");
        }
        jsonGenerator.writeEndObject();
        jsonGenerator.flush();
        jsonGenerator.close();
    }

    private JsonGenerator createJsonGenerator(StringWriter out) throws IOException {
        return JSON_FACTORY.createGenerator(out);
    }

    public abstract java.util.List<String> writeEvent(RawHBaseWALEvent event);
}
