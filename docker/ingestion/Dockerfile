FROM python:3.11-slim




RUN apt-get update && apt-get install -y --no-install-recommends \
   curl \
   ca-certificates \
   && rm -rf /var/lib/apt/lists/*




WORKDIR /app




COPY docker/ingestion/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt




ENV PYTHONUNBUFFERED=1 \
    APP_ENV=dev \
    PYTHONPATH=/app





RUN useradd -m -u 1002 ingestion && \
   chown -R ingestion:ingestion /app




USER ingestion




CMD ["python"]