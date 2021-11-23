package org.apache.spark.sql.mlsql.sources.mysql.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import org.apache.spark.sql.execution.streaming.LongOffset;
import org.apache.spark.sql.execution.streaming.Offset;
import org.apache.spark.streaming.RawEvent;

public class RawBinlogEvent implements RawEvent {

    private Event event;
    private String binlogFilename;
    private Long pos;

    public TableInfo getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    private TableInfo tableInfo;

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    private String eventType;

    public Long getPos() {
        return pos;
    }

    public void setPos(Long pos) {
        this.pos = pos;
    }


    public RawBinlogEvent(Event event, TableInfo tableInfo, String binlogFilename, String eventType, long pos) {
        this.event = event;
        this.binlogFilename = binlogFilename;
        this.pos = pos;
        this.eventType = eventType;
        this.tableInfo = tableInfo;
    }

    public Long getTimestamp() {
        EventHeaderV4 header = event.getHeader();
        return header.getTimestamp();
    }

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public String getBinlogFilename() {
        return binlogFilename;
    }

    @Override
    public String key() {
        return "";
    }

    @Override
    public Offset pos() {
        return LongOffset.apply(BinlogOffset.fromFileAndPos(binlogFilename, pos).offset());
    }
}
