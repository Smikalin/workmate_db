# Multi-stage Dockerfile для SPIMEX парсера
FROM python:3.9-slim as base

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Создаем рабочую директорию
WORKDIR /app

# Копируем requirements.txt и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код
COPY . .

# Создаем пользователя для безопасности
RUN useradd -m -u 1000 spimex && chown -R spimex:spimex /app
USER spimex

# Устанавливаем переменные окружения
ENV PYTHONPATH="/app"
ENV PYTHONUNBUFFERED=1

# Синхронный парсер
FROM base as sync
WORKDIR /app/sync
CMD ["python", "run_loader.py"]

# Асинхронный парсер
FROM base as async
WORKDIR /app/async_parser
CMD ["python", "run_loader.py"]

# API сервер (интегрированный в async_parser)
FROM base as api
WORKDIR /app/async_parser
EXPOSE 8000
CMD ["python", "run_api.py"]

# Бенчмарк
FROM base as benchmark
WORKDIR /app
CMD ["python", "benchmark.py"]


