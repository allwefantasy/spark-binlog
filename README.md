# Spark MySQL Binlog Library

A library for querying MySQL Binlog with Apache Spark structure streaming, 
for Spark SQL , DataFrames and [MLSQL](http://www.mlsql.tech).

1. [jianshu: How spark-binlog works](https://www.jianshu.com/p/e7c3e84a0ea7)
2. [medium: How spark-binlog works](https://medium.com/@williamsmith_74955/how-spark-binlog-works-323c16fb1498)
  
## Requirements

This library requires Spark 2.4+ (tested).
Some older versions of Spark may work too but they are not officially supported.

## Liking 

You can link against this library in your program at the following coordinates:

### Scala 2.11

```sql
groupId: tech.mlsql
artifactId: spark-binlog_2.11
version: 0.2.1
```

## Limitation

1. MySQL BinlogFormat should be set as "Row"
2. Version 0.1.1 only support insert/update/delete events. The other events will ignore.

## Usage


DataFrame:

```scala
val spark: SparkSession = ???

val df = spark.readStream.
format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource").
option("host","127.0.0.1").
option("port","3306").
option("userName","xxxxx").
option("password","xxxxx").
option("databaseNamePattern","xxxxx").
option("tableNamePattern","xxxxx").
optioin("binlogIndex","4").
optioin("binlogFileOffset","4").
load()


df.writeStream.
format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
checkpointLocation("/tmp/cpl-binlog2")
.mode(OutputMode.Append).save("/tmp/binlog1/table1")

```



MLSQL:

```sql
set streamName="binlog";

load binlog.`` where 
host="127.0.0.1"
and port="3306"
and userName="xxx"
and password="xxxx"
and bingLogNamePrefix="mysql-bin"
and startingOffsets="40000000000004"
and databaseNamePattern="mlsql_console"
and tableNamePattern="script_file"
as table1;
```

startingOffsetscan be replaced by `binlogIndex` and `binlogFileOffset`.

`startingOffsets="40000000000004"`  equals `binlogIndex="4" and binlogFileOffset="4"`

Notice that `binlogFileOffset` should be 4 in most case unless you have copied the startingOffsets from CK or logs.

[MLSQL Example](http://docs.mlsql.tech/en/guide/stream/binlog.html)

## How to get the initial offset 

If you are the first time to start spark-binlog, use command like following to get the offset you want:

```
mysqlbinlog \ 
--start-datetime="2019-06-19 01:00:00" \ 
--stop-datetime="2019-06-20 23:00:00" \ 
--base64-output=decode-rows \
-vv  master-bin.000004

```  

## Questions

People may meet some log like following:

```
Trying to restore lost connectioin to .....
Connected to ....
```

Please check the server_id is configured in my.cnf of your MySQL Server. 
  

 





