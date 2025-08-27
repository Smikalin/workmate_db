# 🐳 SPIMEX Parser Docker Guide

Полное руководство по запуску SPIMEX парсера в Docker контейнерах.

## 📋 Содержание

- [Быстрый старт](#быстрый-старт)
- [Архитектура](#архитектура)
- [Установка](#установка)
- [Конфигурация](#конфигурация)
- [Запуск сервисов](#запуск-сервисов)
- [Управление](#управление)
- [Отладка](#отладка)
- [Производительность](#производительность)

## 🚀 Быстрый старт

```bash
# 1. Сборка образов
./docker-run.sh build

# 2. Запуск PostgreSQL
./docker-run.sh start

# 3. Инициализация базы данных
./docker-run.sh init-async

# 4. Запуск асинхронного парсера
./docker-run.sh async

# 5. Запуск бенчмарка (опционально)
./docker-run.sh benchmark
```

## 🏗️ Архитектура

### Контейнеры

| Контейнер              | Описание               | Порты |
|------------------------|------------------------|-------|
| `spimex_postgres`      | PostgreSQL 15          | 5432  |
| `spimex_sync_db_init`  | Инициализация sync БД  |   -   |
| `spimex_async_db_init` | Инициализация async БД |   -   |
| `spimex_sync_parser`   | Синхронный парсер      |   -   |
| `spimex_async_parser`  | Асинхронный парсер     |   -   |
| `spimex_benchmark`     | Бенчмарк тесты         |   -   |

### Базы данных

- **spimex_sync** - для синхронного парсера
- **spimex_async** - для асинхронного парсера


### Клонирование и настройка

```bash
# Переход в директорию проекта
cd spimex/

# Настройка прав для скриптов (Linux/Mac)
chmod +x docker-run.sh

# Копирование переменных окружения
cp docker.env .env  # опционально
```

## ⚙️ Конфигурация

### Переменные окружения

Файл `docker.env` содержит основные настройки:

```env
# База данных PostgreSQL
DB_HOST=postgres
DB_PORT=5432
DB_USER=spimex_user
DB_PASS=spimex_password

# Названия баз данных
SYNC_DB_NAME=spimex_sync
ASYNC_DB_NAME=spimex_async
```

## 🚀 Запуск сервисов

### Способ 1: Скрипты (рекомендуется)

**Windows (Git Bash):**
```bash
./docker-run.sh build
./docker-run.sh start
./docker-run.sh init-async
./docker-run.sh async
```

## 🎛️ Управление

### Просмотр логов

```bash
# Все логи
./docker-run.sh logs

# Логи конкретного сервиса
./docker-run.sh logs postgres
./docker-run.sh logs async-parser
```

### Статус контейнеров

```bash
./docker-run.sh status
```

### Вход в контейнер

```bash
# Postgres
./docker-run.sh shell postgres

# Парсер (если запущен)
./docker-run.sh shell async-parser
```

### Перезапуск сервисов

```bash
# Перезапуск БД
docker-compose --env-file docker.env restart postgres

# Пересборка и перезапуск парсера
./docker-run.sh build
./docker-run.sh async
```

## 🐛 Отладка

### Проверка подключения к БД

```bash
# Вход в PostgreSQL
./docker-run.sh shell postgres
psql -U spimex_user -d spimex_async

# Проверка таблиц
\dt
```

## 🧹 Очистка

### Остановка сервисов

```bash
./docker-run.sh stop
```

### Полная очистка

```bash
# Удаление контейнеров, образов и томов
./docker-run.sh clean


