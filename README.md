# cubefs-hadoop
Hadoop API package support mulit data layer such as blobstore (Erasure-Code) to adapte to datalake. 

# Compile

`mvn package -Dmaven.test.skip=true`

*Notes: You are suggested to skip the test.*

# Deploy

When using the SDK, you need to use two jars and one so and one profile.

**Dependent packages**

1. cubefs-hadoop-x.x.x.jar(build by mvn pakcage)
2. libcfs.so(build from cubefs/libsdk)
3. jna-x.x.x.jar(you can find it in the maven repository)

**Configuration**

Modify the configuration file(core-site.xml or hdfs-site.xml) and add the following items to it.

```xml
<property>
	<name>fs.cfs.impl</name>
	<value>io.cubefs.CfsFileSystem</value>
</property>

<property>
	<name>cfs.master.address</name>
	<value>your.master.address[ip:port,ip:port,ip:port]</value>
</property>

<property>
	<name>cfs.log.dir</name>   
	<value>your.log.dir[/tmp/cfs-access-log]</value>
</property>

<property>
	<name>cfs.log.level</name> 
	<value>INFO</value>
</property>

<property>
    <name>cfs.access.key</name>
    <value>your.access.key</value>
</property>

<property>
    <name>cfs.secret.key</name>
    <value>your.secret.key</value>
</property>

<property>
	<name>cfs.min.buffersize</name>
	<value>8388608</value>
</property>
```

## HDFS Shell on CubeFS

1. Put the two jars to $HADOOP_HOME/share/hadoop/common/lib
2. Put the so to $HADOOP_HOME/lib/native
3. Modify the configuration file $HADOOP_HOME/etc/hadoop/core-site.xml

## YARN on CubeFS

Same to HDFS Shell on CubeFS.

## Spark on CubeFS

1. Put the three dependent packages to $SPARK_HOME/jars

## Hive on CubeFS

Same to HDFS Shell on CubeFS.

## Presto on CubeFS

1. Put the three dependent packages to $PRESTO_HOME/plugin/hive-hadoop2
2. Put the two jars to $TRINO_HOME/plugin/iceberg for iceberg table
3. Link libcfs.so (ln -s $PRESTO_HOME/plugin/hive-hadoop2/libcfs.so /usr/lib; sudo ldconfig)

## Flink on CubeFS

1. Put the three dependent packages to $FLINK_HOME/lib
2. Link libcfs.so (ln -s $FLINK_HOME/lib/libcfs.so /usr/lib; sudo ldconfig)






