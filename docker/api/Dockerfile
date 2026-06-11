FROM python:3.11-slim




RUN apt-get update && apt-get install -y --no-install-recommends \
   curl \
   ca-certificates \
   && rm -rf /var/lib/apt/lists/\*





WORKDIR /app




COPY docker/api/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt





RUN useradd -m -u 1003 apiuser





COPY --chown=apiuser:apiuser api /app/api





ENV PYTHONUNBUFFERED=1 \

   APP_ENV=dev \
   PYTHONPATH=/app





USER apiuser





EXPOSE 8000





HEALTHCHECK CMD curl --fail http://localhost:8000/health || exit 1





CMD ["uvicorn", "api.app.main:app", "--host", "0.0.0.0", "--port", "8000"]