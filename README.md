# Spark Binlog Library

A library for querying Binlog with Apache Spark structure streaming, 
for Spark SQL , DataFrames and [MLSQL](http://www.mlsql.tech).

1. [jianshu: How spark-binlog works](https://www.jianshu.com/p/e7c3e84a0ea7)
2. [medium: How spark-binlog works](https://medium.com/@williamsmith_74955/how-spark-binlog-works-323c16fb1498)
  
## Requirements

This library requires Spark 2.4+ (tested).
Some older versions of Spark may work too but they are not officially supported.

## Linking 

You can link against this library in your program at the following coordinates:

### Scala 2.11

```sql
groupId: tech.mlsql
artifactId: spark-binlog_2.11
version: 0.2.2-SNAPSHOT
```

## Limitation

1. Version 0.2.2-SNAPSHOT only support insert/update/delete events. The other events will ignore.
2. Only MySQL Binlog is supported in version 0.2.2-SNAPSHOT

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
option("bingLogNamePrefix","mysql-bin").
option("databaseNamePattern","mlsql_console").
option("tableNamePattern","script_file").
option("binlogIndex","4").
option("binlogFileOffset","4").
load()


df.writeStream.
format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").  
option("__path__","/tmp/sync/tables").
option("mode","Append").
option("idCols","id").
option("duration","5").
option("syncType","binlog").
option("checkpointLocation","/tmp/cpl-binlog2").
option("path","{db}/{table}").
outputmode(OutputMode.Append)...

```



MLSQL:

```sql
set streamName="binlog";

load binlog.`` where 
host="127.0.0.1"
and port="3306"
and userName="xxxxx"
and password="xxxxx"
and bingLogNamePrefix="mysql-bin"
and binlogIndex="4"
and binlogFileOffset="4"
and databaseNamePattern="mlsql_console"
and tableNamePattern="script_file"
as table1;

save append table1  
as rate.`mysql_{db}.{table}` 
options mode="Append"
and idCols="id"
and duration="5"
and syncType="binlog"
and checkpointLocation="/tmp/cpl-binlog2";
```


[MLSQL Example](http://docs.mlsql.tech/en/guide/stream/binlog.html)

## RoadMap

Until 0.2.2-SNAPSHOT, spark-binlog only supports binlog for MySQL.
We hope we can support more DBs including traditional DB e.g Oracle and 
NoSQL e.g. HBase(WAL),ES,Cassandra in future.  


## How to get the initial offset 

If you are the first time to start spark-binlog, use command like following to get the offset you want:

```
mysql> show master status;
+------------------+----------+--------------+------------------+-------------------+
| File             | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set |
+------------------+----------+--------------+------------------+-------------------+
| mysql-bin.000014 | 34913156 |              |                  |                   |
+------------------+----------+--------------+------------------+-------------------+
1 row in set (0.04 sec)
```  

In this example, we knows that:

```       
bingLogNamePrefix      binlogFileOffset   binlogFileOffset
mysql-bin        .     000014             34913156
```

this means you should configure parameters like this: 

```
bingLogNamePrefix="mysql-bin"
binlogFileOffset="14"
binlogFileOffset="34913156"
```


Or you can use `mysqlbinlog` command.
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
  

 





