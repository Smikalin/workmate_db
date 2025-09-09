set -e

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функция для вывода заголовков
print_header() {
    echo -e "${BLUE}🐳 SPIMEX Docker Manager${NC}"
    echo -e "${BLUE}========================${NC}"
}

# Функция для вывода помощи
show_help() {
    print_header
    echo "Использование: $0 [КОМАНДА]"
    echo ""
    echo "Команды:"
    echo "  build      - Сборка всех образов"
    echo "  start      - Запуск PostgreSQL"
    echo "  init-sync  - Инициализация синхронной базы данных"
    echo "  init-async - Инициализация асинхронной базы данных"
    echo "  init-all   - Инициализация обеих баз данных"
    echo "  sync       - Запуск синхронного парсера"
    echo "  async      - Запуск асинхронного парсера"
    echo "  api        - Запуск API микросервиса"
    echo "  benchmark  - Запуск бенчмарка"
    echo "  stop       - Остановка всех контейнеров"
    echo "  clean      - Удаление всех контейнеров и образов"
    echo "  logs       - Просмотр логов"
    echo "  status     - Статус контейнеров"
    echo "  shell      - Вход в контейнер"
    echo ""
    echo "Примеры:"
    echo "  $0 build && $0 start && $0 init-all"
    echo "  $0 init-sync && $0 sync"
    echo "  $0 init-async && $0 async"
    echo "  $0 init-async && $0 api"
    echo "  $0 benchmark"
    echo "  $0 logs sync-parser"
}

# Функция для сборки образов
build_images() {
    echo -e "${GREEN}🔨 Сборка Docker образов...${NC}"
    docker-compose --env-file docker.env build
    echo -e "${GREEN}✅ Образы собраны успешно${NC}"
}

# Функция для запуска PostgreSQL
start_database() {
    echo -e "${GREEN}🚀 Запуск PostgreSQL...${NC}"
    docker-compose --env-file docker.env up -d postgres
    echo -e "${YELLOW}⏳ Ожидание запуска PostgreSQL...${NC}"
    sleep 10
    echo -e "${GREEN}✅ PostgreSQL готов к работе${NC}"
    echo -e "${CYAN}💡 Используйте './docker-run.sh init-sync' или './docker-run.sh init-async' для инициализации БД${NC}"
}

# Функция для запуска синхронного парсера
run_sync() {
    echo -e "${GREEN}🔄 Запуск синхронного парсера...${NC}"
    docker-compose --env-file docker.env --profile sync up sync-parser
}

# Функция для запуска асинхронного парсера
run_async() {
    echo -e "${GREEN}⚡ Запуск асинхронного парсера...${NC}"
    docker-compose --env-file docker.env --profile async up async-parser
}

# Функция для запуска API микросервиса
run_api() {
    echo -e "${GREEN}🚀 Запуск API микросервиса...${NC}"
    echo -e "${YELLOW}📡 API будет доступен на http://localhost:18000${NC}"
    echo -e "${YELLOW}📖 Документация: http://localhost:18000/docs${NC}"
    docker-compose --env-file docker.env --profile api up redis api
}

# Функция для запуска бенчмарка
run_benchmark() {
    echo -e "${GREEN}📊 Запуск бенчмарка...${NC}"
    docker-compose --env-file docker.env --profile benchmark up benchmark
}

# Функция для остановки контейнеров
stop_containers() {
    echo -e "${YELLOW}🛑 Остановка всех контейнеров...${NC}"
    docker-compose --env-file docker.env down
    echo -e "${GREEN}✅ Контейнеры остановлены${NC}"
}

# Функция для очистки
clean_all() {
    echo -e "${RED}🧹 Удаление всех контейнеров и образов...${NC}"
    docker-compose --env-file docker.env down --rmi all --volumes --remove-orphans
    echo -e "${GREEN}✅ Очистка завершена${NC}"
}

# Функция для просмотра логов
show_logs() {
    if [ -z "$2" ]; then
        docker-compose --env-file docker.env logs -f
    else
        docker-compose --env-file docker.env logs -f "$2"
    fi
}

# Функция для показа статуса
show_status() {
    echo -e "${BLUE}📊 Статус контейнеров:${NC}"
    docker-compose --env-file docker.env ps
}

# Функция для входа в контейнер
enter_shell() {
    if [ -z "$2" ]; then
        echo -e "${RED}❌ Укажите имя контейнера${NC}"
        echo "Доступные контейнеры: postgres, redis, sync-parser, async-parser, api"
        exit 1
    fi
    docker-compose --env-file docker.env exec "$2" /bin/bash
}

# Основная логика
case "${1:-help}" in
    build)
        build_images
        ;;
    start)
        start_database
        ;;
    init-sync)
        echo "🔧 Инициализация только синхронной базы данных..."
        docker-compose --env-file docker.env --profile sync-init run --rm sync-db-init
        ;;
    init-async)
        echo "🚀 Инициализация только асинхронной базы данных..."
        docker-compose --env-file docker.env --profile async-init run --rm async-db-init
        ;;
    init-all)
        echo "🏗️ Инициализация обеих баз данных..."
        echo "🔧 Синхронная БД..."
        docker-compose --env-file docker.env --profile sync-init run --rm sync-db-init
        echo "🚀 Асинхронная БД..."
        docker-compose --env-file docker.env --profile async-init run --rm async-db-init
        echo "✅ Обе базы данных инициализированы!"
        ;;
    sync)
        run_sync
        ;;
    async)
        run_async
        ;;
    api)
        run_api
        ;;
    benchmark)
        run_benchmark
        ;;
    stop)
        stop_containers
        ;;
    clean)
        clean_all
        ;;
    logs)
        show_logs "$@"
        ;;
    status)
        show_status
        ;;
    shell)
        enter_shell "$@"
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo -e "${RED}❌ Неизвестная команда: $1${NC}"
        show_help
        exit 1
        ;;
esac
