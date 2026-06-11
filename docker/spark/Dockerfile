FROM apache/spark:3.5.2

# 1. Switch to root (install deps)
USER root

# 2. Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    bash \
    && rm -rf /var/lib/apt/lists/*

# 3. Python dependencies (for Spark jobs)
RUN pip install --upgrade pip setuptools wheel
COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 4. Copy Spark configuration
COPY docker/spark/conf/ /opt/spark/conf/

# 5. Copy JARs (flat structure for Spark)
COPY docker/spark/jars/hadoop-aws/*.jar /opt/spark/jars/
COPY docker/spark/jars/aws-sdk/*.jar /opt/spark/jars/
COPY docker/spark/jars/hudi/*.jar /opt/spark/jars/
COPY docker/spark/jars/elasticsearch/*.jar /opt/spark/jars/

# 6. Copy entrypoint script
COPY docker/spark/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh


# 8. Switch back to Spark user
RUN mkdir -p /opt/spark/work /opt/spark/tmp \
 && chown -R 185:185 /opt/spark/work /opt/spark/tmp

# 7. Environment variables
ENV APP_ENV=dev \
    PYTHONUNBUFFERED=1 \
    SPARK_NO_DAEMONIZE=true \
    SPARK_WORK_DIR=/opt/spark/work


USER 185

# 9. Entrypoint
ENTRYPOINT ["/entrypoint.sh"]