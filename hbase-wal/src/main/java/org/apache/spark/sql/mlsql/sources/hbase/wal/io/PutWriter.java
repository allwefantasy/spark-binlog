package org.apache.spark.sql.mlsql.sources.hbase.wal.io;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.mlsql.sources.hbase.wal.RawHBaseWALEvent;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 12/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
public class PutWriter extends AbstractEventWriter {
    @Override
    public List<String> writeEvent(RawHBaseWALEvent event) {
        List<String> items = new ArrayList<>();
        try {
            StringWriter writer = new StringWriter();
            startJson(writer, event);

            jsonGenerator.writeArrayFieldStart("rows");
            jsonGenerator.writeStartObject();
            Put put = event.put();
            Map.Entry<byte[], List<Cell>> entry = put.getFamilyCellMap().firstEntry();
            Cell cell = entry.getValue().get(0);
            String f = new String(cell.getFamilyArray());
            String col = new String(cell.getQualifierArray());

            jsonGenerator.writeObjectField("rowkey", new String(entry.getKey()));
            jsonGenerator.writeObjectField(f + ":" + col, Bytes.toString(entry.getValue().get(0).getRowArray()));

            jsonGenerator.writeEndObject();
            jsonGenerator.writeEndArray();

            endJson();
            items.add(writer.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }
        return items;

    }
}
