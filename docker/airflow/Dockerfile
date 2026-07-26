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

# 3b. JARs de connecteurs — mêmes fichiers que docker/spark/Dockerfile.
# SparkSubmitOperator exécute spark-submit localement dans CE conteneur
# (le driver tourne ici), donc il lui faut aussi S3A/Hudi/Elasticsearch,
# même si l'exécution distribuée se fait sur le vrai cluster spark-master.
COPY docker/spark/jars/hadoop-aws/*.jar /opt/spark/jars/
COPY docker/spark/jars/aws-sdk/*.jar /opt/spark/jars/
COPY docker/spark/jars/hudi/*.jar /opt/spark/jars/
COPY docker/spark/jars/elasticsearch/*.jar /opt/spark/jars/
RUN chown -R airflow:root /opt/spark/jars

RUN apt-get update && \
    apt-get install -y docker-compose-plugin

USER airflow

# 4. Python dependencies
COPY docker/airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt apache-airflow-providers-apache-spark==4.7.1