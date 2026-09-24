FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./

COPY scripts/entrypoint-worker.sh /entrypoint-worker.sh
RUN chmod +x /entrypoint-worker.sh

ENV DUCKDB_FILE=/data/worker.duckdb

EXPOSE 9494

ENTRYPOINT ["/entrypoint-worker.sh"]