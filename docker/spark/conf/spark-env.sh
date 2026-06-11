#!/bin/bash



# JAVA (si nécessaire dans image custom)



export JAVA_HOME=/opt/java/openjdk





# Spark env

export SPARK_MODE=standalone



# app env (prod/dev)

export APP_ENV=${APP_ENV:-dev}



# MINIO ENDPOINT (utile pour jobs Python/Spark)

export MINIO_ENDPOINT=http://minio:9000



# HIVE METASTORE

export HIVE_METASTORE_URI=thrift://hive-metastore:9083



# MINIO SECRETS BINDING (dans env-dev) injecter depuis docker-compose

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}

export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
