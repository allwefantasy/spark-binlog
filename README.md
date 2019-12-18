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

This is the latest stable version for MySQL binlog:

```
groupId: tech.mlsql
artifactId: spark-binlog_2.11
version: 0.2.2
```

This is the latest SNAPSHOT versions.

MySQL Binlog:

```      
groupId: tech.mlsql
artifactId: mysql-binlog_2.11
version: 1.0.0-SNAPSHOT
```

HBase WAL:

```      
groupId: tech.mlsql
artifactId: hbase-wal_2.11
version: 1.0.0-SNAPSHOT
```

## Limitation

1. mysql-binlog only support insert/update/delete events. The other events will ignore.
2. hbase-wal only support Put/Delete events. The other events will ignore.

## MySQL Binlog Usage

The example should work with [delta-plus](https://github.com/allwefantasy/delta-plus)

MLSQL Code:

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

DataFrame Code:

```scala
val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Binlog2DeltaTest")
      .getOrCreate()

val df = spark.readStream.
  format("org.apache.spark.sql.mlsql.sources.MLSQLBinLogDataSource").
  option("host","127.0.0.1").
  option("port","3306").
  option("userName","root").
  option("password","123456").
  option("databaseNamePattern","test").
  option("tableNamePattern","mlsql_binlog").
  option("bingLogNamePrefix","mysql-bin").
  option("binlogIndex","10").
  option("binlogFileOffset","90840").
  load()

val query = df.writeStream.
  format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
  option("__path__","/tmp/datahouse/{db}/{table}").
  option("path","{db}/{table}").
  option("mode","Append").
  option("idCols","id").
  option("duration","3").
  option("syncType","binlog").
  option("checkpointLocation", "/tmp/cpl-binlog2").
  outputMode("append")
  .trigger(Trigger.ProcessingTime("3 seconds"))
  .start()

query.awaitTermination()

```


Before you run the streaming application, make sure you have fully sync the table 

MLSQL Code:

```sql
connect jdbc where
 url="jdbc:mysql://127.0.0.1:3306/mlsql_console?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
 and driver="com.mysql.jdbc.Driver"
 and user="xxxxx"
 and password="xxxx"
 as db_cool;
 
load jdbc.`db_cool.script_file`  as script_file;

run script_file as TableRepartition.`` where partitionNum="2" and partitionType="range" and partitionCols="id"
as rep_script_file;

save overwrite rep_script_file as delta.`mysql_mlsql_console.script_file` ;

load delta.`mysql_mlsql_console.script_file`  as output;
```

DataFrame Code:

```scala
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder()
  .master("local[*]")
  .appName("wow")
  .getOrCreate()

val mysqlConf = Map(
  "url" -> "jdbc:mysql://localhost:3306/mlsql_console?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false",
  "driver" -> "com.mysql.jdbc.Driver",
  "user" -> "xxxxx",
  "password" -> "xxxx",
  "dbtable" -> "script_file"
)

import org.apache.spark.sql.functions.col
var df = spark.read.format("jdbc").options(mysqlConf).load()
df = df.repartitionByRange(2, col("id") )
df.write
  .format("org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource").
  mode("overwrite").
  save("/tmp/datahouse/mlsql_console/script_file")
spark.close()
```

## HBase WAL Usage

DataFrame code:

```scala
val spark = SparkSession.builder()
      .master("local[*]")
      .appName("HBase WAL Sync")
      .getOrCreate()

    val df = spark.readStream.
      format("org.apache.spark.sql.mlsql.sources.hbase.MLSQLHBaseWALDataSource").
      option("walLogPath", "/Users/allwefantasy/Softwares/hbase-2.1.8/WALs").
      option("oldWALLogPath", "/Users/allwefantasy/Softwares/hbase-2.1.8/oldWALs").
      option("startTime", "1").
      option("databaseNamePattern", "test").
      option("tableNamePattern", "mlsql_binlog").
      load()

    val query = df.writeStream.
      format("console").
      option("mode", "Append").
      option("truncate", "false").
      option("numRows", "100000").
      option("checkpointLocation", "/tmp/cpl-binlog25").
      outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
```

## RoadMap

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
  

 





