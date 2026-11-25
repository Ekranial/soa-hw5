# Build context: корень проекта (нужна папка clickhouse/migrations)
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY aggregation-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY clickhouse/migrations/ /app/clickhouse/migrations/
COPY aggregation-service/app/ /app/app/

EXPOSE 8001
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
