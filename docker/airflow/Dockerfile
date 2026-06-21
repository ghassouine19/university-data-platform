FROM apache/airflow:2.9.1

USER root

# 1. System dependencies + Java 17
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

# 2. Variables Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_VERSION=3.5.2 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PATH="${PATH}:/opt/spark/bin"

# 3. Téléchargement et installation de Spark
COPY docker/airflow/spark-3.5.2-bin-hadoop3.tgz /tmp/spark.tgz
RUN tar -xz -C /opt/ -f /tmp/spark.tgz \
    && mv "/opt/spark-3.5.2-bin-hadoop3" "${SPARK_HOME}" \
    && chown -R airflow:root "${SPARK_HOME}" \
    && rm /tmp/spark.tgz

USER airflow

# 4. Python dependencies
COPY docker/airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt