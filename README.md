# Spark MySQL Binlog Library

A library for querying MySQL Binlog with Apache Spark structure streaming, 
for Spark SQL , DataFrames and [MLSQL](http://www.mlsql.tech).
  
## Requirements

This library requires Spark 2.4+ (tested).
Some older versions of Spark may work too but they are not officially supported.

## Liking 

You can link against this library in your program at the following coordinates:

### Scala 2.11

```sql
groupId: tech.mlsql
artifactId: spark-binlog_2.11
version: 0.1.2
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
load()

df.writeStream.format("delta")....

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





