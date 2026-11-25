# Build context: корень проекта (нужна папка schemas/)
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY movie-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schemas/ /app/schemas/
COPY movie-service/app/ /app/app/

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
