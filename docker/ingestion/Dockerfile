FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive

# VARIABLE CRITIQUE : Force Playwright à stocker Chromium dans un dossier global partagé
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY docker/ingestion/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# L'installation se fait en root dans le dossier global /opt/playwright
RUN apt-get update && \
    playwright install chromium --with-deps && \
    rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    APP_ENV=dev \
    PYTHONPATH=/app

RUN useradd -m -u 1002 ingestion && \
    chown -R ingestion:ingestion /app && \
    chown -R ingestion:ingestion /opt/playwright

USER ingestion

CMD ["python"]
