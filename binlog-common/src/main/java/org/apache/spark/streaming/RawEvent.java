package org.apache.spark.streaming;

import org.apache.spark.sql.execution.streaming.Offset;

import java.io.Serializable;

/**
 * 11/12/2019 WilliamZhu(allwefantasy@gmail.com)
 */
public interface RawEvent extends Serializable {
    public String key();

    public Offset pos();
}
