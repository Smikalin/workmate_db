EXCEL_ENGINE = "xlrd"
COLUMN_PATTERNS = {
    "exchange_product_id": ["код", "инструмента"],
    "exchange_product_name": ["наименование", "инструмента"],
    "delivery_basis_name": ["базис", "поставки"],
    "volume": ["объем", "договоров", "единицах"],
    "total": ["обьем", "договоров", "руб"],
    "count": ["количество", "договоров", "шт"],
}

DATE_FORMAT = "%Y-%m-%d"
DATE_FORMAT_SPIMEX = "%Y%m%d"

BASE_URL = "https://spimex.com/markets/oil_products/trades/results/"

PAGE_URL_PATTERN = "?page=page-{page_num}"

FILE_URL_PATTERN = r'href="([^"]*oil_xls_{date_formatted}\d{{6}}\.xls[^"]*)"'

ALL_FILES_PATTERN = r'href="([^"]*oil_xls_(\d{8})\d{6}\.xls[^"]*)"'

RECORDS_TO_SAVE = [
    "exchange_product_id",
    "exchange_product_name",
    "oil_id",
    "delivery_basis_id",
    "delivery_basis_name",
    "delivery_type_id",
    "volume",
    "total",
    "count",
    "date",
    "created_on",
    "updated_on",
]

# Строка поиска секции "Метрическая тонна"
METRIC_TON_MARKER = "Единица измерения: Метрическая тонна"

# Числовые столбцы для обработки
NUMERIC_COLUMNS = ["volume", "total", "count"]

# Максимальное количество страниц для проверки
MAX_PAGES_TO_CHECK = 64

# Таймауты для HTTP запросов
HTTP_TIMEOUT = 10
