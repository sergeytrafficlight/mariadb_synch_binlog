# MariaDB Binlog Sync Engine

Асинхронный движок репликации данных из MariaDB в произвольные системы хранения через обработку бинарных логов.

## Основные возможности

- **Полная первоначальная синхронизация** — параллельное чтение всех данных из указанных таблиц
- **Инкрементальная синхронизация** — непрерывная потоковая обработка бинарных логов
- **Точное позиционирование** — сохранение и восстановление позиции в бинарном логе
- **Гибкая архитектура** — плагинная система для обработки событий
- **Мониторинг** — UNIX socket для проверки статуса и отставания
- **Проверка конфигурации** — автоматическая проверка прав доступа и настроек MariaDB

## Архитектура

Проект состоит из двух основных компонентов:

1. **Движок синхронизации** (`src/engine.py`) — управляет процессом репликации
2. **Плагин обработки событий** — пользовательский модуль, определяющий логику сохранения данных

## Быстрый старт

### Установка зависимостей

```bash
pip install -r requirements.txt
```

### Настройка MariaDB

1. Создайте пользователя с минимальными необходимыми правами:
```sql
CREATE USER 'binlog_reader'@'%' IDENTIFIED BY 'password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'binlog_reader'@'%';
GRANT SELECT ON your_database.* TO 'binlog_reader'@'%';
```

2. Убедитесь, что в `my.cnf` включены необходимые настройки:
```ini
[mysqld]
log_bin = ON
binlog_format = ROW
binlog_row_image = FULL
binlog_row_metadata = FULL
binlog_gtid_index = ON
gtid_strict_mode = ON
server_id = 1
```

### Конфигурация

Отредактируйте `config/config.py`:

```python
MYSQL_SETTINGS = {
    "host": "localhost",
    "port": 3306,
    "user": "binlog_reader",
    "passwd": "password",
}

APP_SETTINGS = {
    'db_name': 'your_database',
    'init_tables': ['table1', 'table2'],  # таблицы для полной синхронизации
    'scan_tables': ['table1', 'table2'],  # таблицы для инкрементальной синхронизации
    'full_regeneration_threads_count': 4,
    'full_regeneration_batch_len': 1000,
    'health_socket': './common/health.sock',
    'binlog_file': './common/binlog.pos',
    'handle_events_plugin': 'your_plugin_module.plugin'  # путь к вашему плагину
}
```

### Создание плагина обработки событий

Создайте модуль с обязательными функциями:

```python
# plugins/my_plugin.py

def init(binlog_position_save_function):
    """Инициализация плагина"""
    pass

def initiate_full_regeneration():
    """Начало полной синхронизации"""
    pass

def finished_full_regeneration():
    """Завершение полной синхронизации"""
    pass

def initiate_synch_mode():
    """Начало инкрементальной синхронизации"""
    pass

def tear_down():
    """Завершение работы"""
    pass

def process_event(event_type, schema, table, event, binlog):
    """Обработка события из бинарного лога"""
    # event_type: 'insert', 'update', 'delete'
    # event: данные строки
    # binlog: объект с позицией в бинарном логе
    pass

def XidEvent():
    """Обработка завершения транзакции"""
    pass
```

### Запуск

```bash
python main.py
```

## API плагина

### Обязательные функции

#### `init(binlog_position_save_function)`
Вызывается при инициализации движка. Принимает функцию для сохранения позиции в бинарном логе.

#### `initiate_full_regeneration()`
Вызывается перед началом полной синхронизации.

#### `finished_full_regeneration()`
Вызывается после завершения полной синхронизации.

#### `initiate_synch_mode()`
Вызывается перед началом инкрементальной синхронизации.

#### `tear_down()`
Вызывается при завершении работы движка.

#### `process_event(event_type, schema, table, event, binlog)`
Обрабатывает события из бинарного лога:
- `event_type`: тип события (`'insert'`, `'update'`, `'delete'`)
- `schema`: имя базы данных
- `table`: имя таблицы
- `event`: данные события
- `binlog`: объект `binlog_file` с текущей позицией

#### `XidEvent()`
Вызывается при завершении транзакции. Оптимальное место для фиксации пакетных операций.

## Утилиты

### Класс `insert_buffer`
Буфер для накопления операций вставки перед пакетной записью:
```python
from src.tools import insert_buffer

buffer = insert_buffer(triggering_rows_count=1000)
```

### Проверка состояния
```bash
# Используйте health socket для проверки статуса
socat UNIX-CONNECT:./common/health.sock -
# или используйте функцию get_health_answer()
```

### Сохранение позиции в бинарном логе
```python
binlog = binlog_file('./common/binlog.pos')
binlog.load()  # загрузка сохраненной позиции
binlog.file = 'mysql-bin.000001'
binlog.pos = 123456
binlog.save()  # сохранение позиции
```

## Мониторинг

Движок предоставляет Health API через UNIX socket:

```json
{
  "status": "ok",
  "stage": "SYNCH",
  "init_rows_total": 100000,
  "init_rows_parsed": 75000,
  "server_gtid": "0-1-100",
  "consumer_gtid": "0-1-95",
  "gtid_diff": 5,
  "error": null
}
```

## Тестирование

Запуск тестов:
```bash
python -m pytest tests/
```

Примеры тестовых плагинов находятся в `plugins_test/`.

## Требования к окружению

- Python 3.7+
- MariaDB 10.2+ с включенными бинарными логами
- Права доступа `REPLICATION SLAVE` и `REPLICATION CLIENT`
- Доступ на чтение к реплицируемым таблицам

## Безопасность

- Движок работает только в режиме чтения
- Проверяются права доступа при запуске
- Запрещены привилегии `SUPER`, `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`, `TRUNCATE`

## Примечания

1. Для работы требуется включенный GTID режим в MariaDB
2. Позиция сохраняется атомарно через временный файл
3. При прерывании работы (Ctrl+C) движок завершается корректно, сохраняя позицию
4. Двойное нажатие Ctrl+C приводит к немедленному завершению

## Примеры использования

См. `plugins_test/plugin_test.py` для примера реализации плагина, записывающего данные в ClickHouse.
