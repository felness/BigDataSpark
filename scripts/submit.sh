#!/bin/bash

SPARK_HOME=/opt/bitnami/spark
SCRIPT=/app/main.py
TASK=$1
JARS=/opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/jars/mongo-spark-connector_2.12-10.3.0.jar,/opt/bitnami/spark/jars/mongodb-driver-sync-4.11.2.jar,/opt/bitnami/spark/jars/bson-4.11.2.jar,/opt/bitnami/spark/jars/mongodb-driver-core-4.11.2.jar,/opt/bitnami/spark/jars/spark-cassandra-connector_2.12-3.5.1.jar,/opt/bitnami/spark/jars/clickhouse-jdbc-0.7.1.jar,/opt/bitnami/spark/jars/neo4j-connector-apache-spark_2.12-5.3.0_for_spark_3.jar,/opt/bitnami/spark/jars/neo4j-java-driver-5.14.0.jar,/opt/bitnami/spark/jars/spark-redis_2.12-3.1.0.jar,/opt/bitnami/spark/jars/jedis-4.3.2.jar,/opt/bitnami/spark/jars/httpclient5-5.2.1.jar,/opt/bitnami/spark/jars/cassandra-driver-core-4.17.0.jar
if [ "$TASK" == "transform" ]; then
    $SPARK_HOME/bin/spark-submit \
        --master spark://spark-master:7077 \
        --executor-memory 2g \
        --executor-cores 1 \
        --num-executors 1 \
        --conf spark.sql.shuffle.partitions=10 \
        $SCRIPT snowflake

elif [ "$TASK" == "reports-clickhouse" ]; then
    $SPARK_HOME/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars $JARS \
        --executor-memory 2g \
        --executor-cores 1 \
        --num-executors 1 \
        --conf spark.sql.shuffle.partitions=10 \
        $SCRIPT clickhouse

elif [ "$TASK" == "reports-mongo" ]; then
    $SPARK_HOME/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars $JARS \
        --executor-memory 2g \
        --executor-cores 1 \
        --num-executors 1 \
        --conf spark.sql.shuffle.partitions=10 \
        --conf spark.mongodb.output.uri=mongodb://user:pass@mongo-db:27017/sales_db?authSource=sales_db \
        --conf spark.mongodb.input.uri=mongodb://user:pass@mongo-db:27017/sales_db?authSource=sales_db \
        $SCRIPT mongodb

elif [ "$TASK" == "reports-cassandra" ]; then
    $SPARK_HOME/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars $JARS \
        --executor-memory 2g \
        --executor-cores 1 \
        --num-executors 1 \
        --conf spark.sql.shuffle.partitions=10 \
        --conf spark.cassandra.connection.host=cassandra \
        --conf spark.cassandra.connection.port=9042 \
        --conf spark.cassandra.auth.username=cassandra \
        --conf spark.cassandra.auth.password=cassandra \
        $SCRIPT cassandra

elif [ "$TASK" == "reports-neo4j" ]; then
    $SPARK_HOME/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars $JARS \
        --executor-memory 2g \
        --executor-cores 1 \
        --num-executors 1 \
        --conf spark.sql.shuffle.partitions=10 \
        $SCRIPT neo4j

elif [ "$TASK" == "reports-valkey" ]; then
    $SPARK_HOME/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars $JARS \
        --packages com.redislabs:spark-redis_2.12:2.6.0 \
        --executor-memory 2g \
        --executor-cores 1 \
        --num-executors 1 \
        --conf spark.sql.shuffle.partitions=10 \
        $SCRIPT valkey

else
    echo "Укажите задачу: transform, reports-clickhouse, reports-mongo, reports-cassandra, reports-neo4j, reports-valkey"
    exit 1
fi