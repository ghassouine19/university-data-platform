#!/bin/bash

set -e

echo "====================================="
echo "University Data Platform - Spark Node"
echo "Mode: $SPARK_MODE"
echo "====================================="

if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Master..."
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
        --host 0.0.0.0 \
        --port 7077 \
        --webui-port 8080

elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Starting Spark Worker..."
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --webui-port 8081 \
        ${SPARK_MASTER_URL}

else
    echo "Unknown SPARK_MODE: $SPARK_MODE"
    exit 1
fi