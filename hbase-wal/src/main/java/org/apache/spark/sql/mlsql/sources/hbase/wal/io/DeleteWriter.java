package org.apache.spark.sql.mlsql.sources.hbase.wal.io;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
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
public class DeleteWriter extends AbstractEventWriter {
    @Override
    public List<String> writeEvent(RawHBaseWALEvent event) {
        List<String> items = new ArrayList<>();
        try {
            StringWriter writer = new StringWriter();
            startJson(writer, event);

            jsonGenerator.writeArrayFieldStart("rows");
            jsonGenerator.writeStartObject();
            Delete delete = event.del();

            jsonGenerator.writeObjectField("rowkey", Bytes.toString(delete.getRow()));
            for (Map.Entry<byte[], List<Cell>> entry : delete.getFamilyCellMap().entrySet()) {
                for (Cell cell : entry.getValue()) {
                    String f = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
                    String col = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String dataValue = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    jsonGenerator.writeObjectField(f + ":" + col, dataValue);
                }
            }

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
