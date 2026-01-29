import signal
import os
import time
import pymysql
import logging
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent

logging.getLogger("pymysqlreplication").setLevel(logging.ERROR)

STOP = False
last_sigint = 0
FORCE_EXIT_WINDOW = 1.5



REQUIRED = {
    "REPLICATION SLAVE",
}

ALLOWED_OPTIONAL = {
    "REPLICATION CLIENT",
    "BINLOG MONITOR",
}

FORBIDDEN = {
    "SUPER", "ALL PRIVILEGES",
    "INSERT", "UPDATE", "DELETE",
    "DROP", "ALTER", "CREATE", "TRUNCATE"
}


def handle_stop(signum, frame):
    global STOP, last_sigint

    now = time.time()

    # второй Ctrl+C подряд
    if now - last_sigint < FORCE_EXIT_WINDOW:
        print("\n💥 Force exit (second Ctrl+C)")
        os._exit(130)  # немедленный выход

    last_sigint = now
    STOP = True
    print("\n🛑 Graceful shutdown requested (press Ctrl+C again to force)")

signal.signal(signal.SIGINT, handle_stop)   # Ctrl+C
signal.signal(signal.SIGTERM, handle_stop)

def preflight_check(cursor, mysql_settings):
    def check_grants(cursor):
        cursor.execute("SHOW GRANTS FOR CURRENT_USER")
        grants = [row[0] for row in cursor.fetchall()]
        grants_str = " ".join(grants).upper()
        missing = [g for g in REQUIRED if g not in grants_str]
        if missing:
            raise RuntimeError(f"Missing required privileges: {missing}")

        if not any(opt in grants_str for opt in ALLOWED_OPTIONAL):
            raise RuntimeError(
                f"Missing required privilege: {ALLOWED_OPTIONAL}"
            )

        forbidden = [g for g in FORBIDDEN if g in grants_str]
        if forbidden:
            raise RuntimeError(f"Forbidden privileges detected: {forbidden}")

    def check_variables(cursor):
        cursor.execute("""
            SHOW VARIABLES WHERE Variable_name IN
            ('log_bin','binlog_format','binlog_row_image','server_id')
        """)
        vars = {k.lower(): v for k, v in cursor.fetchall()}

        errors = []

        if vars.get("log_bin") != "ON":
            errors.append("log_bin is OFF")

        if vars.get("binlog_format") != "ROW":
            errors.append("binlog_format != ROW")

        if vars.get("binlog_row_image") != "FULL":
            errors.append("binlog_row_image != FULL")

        if int(vars.get("server_id", 0)) <= 0:
            errors.append("server_id not set")

        if errors:
            raise RuntimeError("; ".join(errors))

    def assert_readonly(cursor):
        try:
            cursor.execute("CREATE TEMPORARY TABLE tmp_test (id INT)")
            raise RuntimeError("User can CREATE tables")
        except Exception:
            pass

    def probe_binlog(mysql_settings):
        """
        Safe check: can we connect to MariaDB binlog as a slave.
        Does NOT consume or advance binlog position.
        """
        stream = BinLogStreamReader(
            connection_settings=mysql_settings,
            server_id=999999,  # временный fake-slave id
            blocking=False,  # не ждём события
            resume_stream=False,  # не сохраняем позицию
            freeze_schema=True,
        )

        try:
            # Просто пытаемся получить первое событие (или None)
            iterator = iter(stream)
            try:
                next(iterator)
            except StopIteration:
                pass

        finally:
            stream.close()

    check_grants(cursor)
    check_variables(cursor)
    assert_readonly(cursor)
    probe_binlog(mysql_settings)

def start_binlog_consumer(mysql_settings, app_settings, stop_event):
    """
    Вечный loop, читаем события из бинлога MariaDB.
    """
    stream = BinLogStreamReader(
        connection_settings=mysql_settings,
        server_id=100001,          # уникальный server_id для consumer
        blocking=False,             # ждём новые события
        resume_stream=True,        # продолжим с последней позиции, если указана
        only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        only_schemas=[app_settings['db_name']],
        freeze_schema=True,        # не модифицируем schema
    )

    print(f"🚀 Binlog consumer started. Symch with [{app_settings['db_name']}] . Waiting for events...")

    try:
        while not (STOP or (stop_event and stop_event.is_set())):
            # вытаскиваем события, если есть
            try:
                event = next(iter(stream))
            except StopIteration:
                time.sleep(0.1)  # пауза, чтобы не жрать CPU
                continue

            schema = event.schema
            table = event.table
            for row in event.rows:
                if isinstance(event, WriteRowsEvent):
                    print(f"[INSERT] {schema}.{table}: {row['values']}")
                elif isinstance(event, UpdateRowsEvent):
                    print(f"[UPDATE] {schema}.{table}: before={row['before_values']} after={row['after_values']}")
                elif isinstance(event, DeleteRowsEvent):
                    print(f"[DELETE] {schema}.{table}: {row['values']}")

    finally:
        stream.close()

def run(stop_event=None):
    from config.config import MYSQL_SETTINGS, APP_SETTINGS
    try:
        print(f"settings")
        print(MYSQL_SETTINGS)
        conn = pymysql.connect(**MYSQL_SETTINGS)
        cursor = conn.cursor()

        preflight_check(cursor, MYSQL_SETTINGS)
        start_binlog_consumer(MYSQL_SETTINGS, APP_SETTINGS, stop_event)

    finally:
        conn.close()