import signal
import os
import time
import pymysql
import logging
import socket
import threading
import importlib
import json
from enum import Enum
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.event import MariadbGtidEvent, XidEvent, QueryEvent
from src.tools import binlog_file, plugin_wrapper, regeneration_threads_controller, get_gtid_diff

logging.getLogger("pymysqlreplication").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s:%(lineno)d  - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

class Stage(Enum):
    INIT = 'INIT'
    REGENERATION = 'REGENERATION'
    SYNCH = 'SYNCH'

USER_FUNC = None
STOP = None
LAST_SIGINT = None
FORCE_EXIT_WINDOW = None
GTID = None
STAGE = None
REGENERATION_CONTROLLER = None
GLOBAL_LOCK = threading.Lock()

def init(MYSQL_SETTINGS, APP_SETTINGS):
    global USER_FUNC, STOP, LAST_SIGINT, FORCE_EXIT_WINDOW, GTID, STAGE, REGENERATION_CONTROLLER
    USER_FUNC = plugin_wrapper(APP_SETTINGS['handle_events_plugin'])
    STOP = False
    LAST_SIGINT = 0
    FORCE_EXIT_WINDOW = 1.5
    GTID = None
    STAGE = Stage.INIT
    REGENERATION_CONTROLLER = regeneration_threads_controller()

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
    global STOP, LAST_SIGINT, user_func
    now = time.time()

    # второй Ctrl+C подряд
    if now - LAST_SIGINT < FORCE_EXIT_WINDOW:
        print("\n💥 Force exit (second Ctrl+C)")
        os._exit(130)  # немедленный выход

    LAST_SIGINT = now
    STOP = True
    print("\n🛑 Graceful shutdown requested (press Ctrl+C again to force)")

signal.signal(signal.SIGINT, handle_stop)   # Ctrl+C
signal.signal(signal.SIGTERM, handle_stop)

def preflight_check(cursor, mysql_settings, app_settings):
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
            SHOW GLOBAL VARIABLES WHERE Variable_name IN
            ('log_bin','binlog_format', 'binlog_row_metadata', 'binlog_row_image','server_id', 'binlog_gtid_index', 'gtid_strict_mode')
        """)
        vars = {k.lower(): v for k, v in cursor.fetchall()}

        errors = []


        if vars.get("log_bin") != "ON":
            errors.append("log_bin is OFF")

        if vars.get("binlog_format") != "ROW":
            errors.append("binlog_format != ROW")

        if vars.get("binlog_row_image") != "FULL":
            errors.append("binlog_row_image != FULL")

        if vars.get("binlog_gtid_index") != "ON":
            errors.append(f"binlog_gtid_index {vars.get('binlog_gtid_index')} != ON")

        if vars.get("gtid_strict_mode") != "ON":
            errors.append(f"gtid_strict_mode {vars.get('gtid_strict_mode')} != ON")

        if vars.get("binlog_row_metadata") != "FULL":
            errors.append(f"binlog_row_metadata {vars.get('binlog_row_metadata')} != FULL")

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
            #auto_position=True,
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

    def check_tables(cursor, db_name, tables):

        cursor.execute(f"SHOW TABLES FROM {db_name};")

        r = cursor.fetchall()
        existing_tables = []
        for t in r:
            existing_tables.append(t[0])

        errors = []
        for t in tables:
            if t not in existing_tables:
                errors.append(t)

        if len(errors):
            raise RuntimeError(f"Can't find tables: {errors}")

    check_grants(cursor)
    check_variables(cursor)
    assert_readonly(cursor)
    probe_binlog(mysql_settings)
    if len(app_settings.get('scan_tables', [])):
        check_tables(cursor, app_settings['db_name'], app_settings['scan_tables'])

def start_binlog_consumer(mysql_settings, app_settings, binlog):
    global USER_FUNC, GTID, GLOBAL_LOCK, STAGE


    USER_FUNC.initiate_synch_mode()
    STAGE = Stage.SYNCH

    binlog_stream = BinLogStreamReader(
        connection_settings=mysql_settings,
        server_id=100001,          # уникальный server_id для consumer
        blocking=False,             # ждём новые события
        resume_stream=True,        # продолжим с последней позиции, если указана
        only_events=[
            WriteRowsEvent,
            UpdateRowsEvent,
            DeleteRowsEvent,
            MariadbGtidEvent,
            XidEvent,
            QueryEvent,
        ],
        only_schemas=[app_settings['db_name']],
        only_tables=app_settings['scan_tables'],
        freeze_schema=True,        # не модифицируем schema
        log_file=binlog.file,
        log_pos=binlog.pos,
    )

    logger.info(f"🚀 Binlog consumer started. Synch with [{app_settings['db_name']}] . Waiting for events...")

    try:

        while not STOP:
            for event in binlog_stream:


                if isinstance(event, MariadbGtidEvent):
                    with GLOBAL_LOCK:
                        GTID = event.gtid
                    continue

                elif isinstance(event, QueryEvent):
                    pass

                elif isinstance(event, XidEvent):
                    USER_FUNC.XidEvent()
                    binlog.file = binlog_stream.log_file
                    binlog.pos = event.packet.log_pos
                    logger.debug(f"save binlog {binlog}")
                    assert binlog.save()

                elif isinstance(event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
                    schema = event.schema
                    table = event.table
                    for row in event.rows:

                        if isinstance(event, WriteRowsEvent):
                            USER_FUNC.process_event('insert', schema, table, row['values'])
                        elif isinstance(event, UpdateRowsEvent):
                            USER_FUNC.process_event('update', schema, table, row)
                        elif isinstance(event, DeleteRowsEvent):
                            USER_FUNC.process_event('delete', schema, table, row)

            time.sleep(0.2)

    finally:
        binlog_stream.close()

def health_server(socket_path, mysql_settings):

    def get_current_gtid():
        conn = pymysql.connect(**mysql_settings)
        cursor = conn.cursor()

        cursor.execute("SELECT @@GLOBAL.gtid_current_pos;")
        r = cursor.fetchall()
        if r:
            r = r[0][0]
        else:
            r = []
        conn.close()
        return r


    global STOP, GLOBAL_LOCK, REGENERATION_CONTROLLER, STAGE
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        server.bind(socket_path)
    except OSError:
        os.remove(socket_path)
        server.bind(socket_path)

    server.listen(1)
    server.settimeout(1.0)

    logger.info("🟢 Health server started")

    try:
        while not STOP:
            try:
                conn, _ = server.accept()
            except socket.timeout:

                continue
            with conn:
                try:
                    current_gtid = get_current_gtid()
                    error = None
                except Exception as e:
                    current_gtid = None
                    error = str(e)

                with GLOBAL_LOCK:
                    init_rows_total, init_rows_parsed = REGENERATION_CONTROLLER.statistic()
                    response = {
                        "status": "ok" if error is None else "error",
                        "stage": str(STAGE),
                        "init_rows_total": init_rows_total,
                        "init_rows_parsed": init_rows_parsed,
                        "server_gtid": current_gtid,
                        "consumer_gtid": GTID,
                        "gtid_diff": get_gtid_diff(GTID, current_gtid),
                        "error": error,
                    }

                conn.sendall((json.dumps(response) + "\n").encode())
    finally:
        server.close()
        if os.path.exists(socket_path):
            os.remove(socket_path)
        logger.info("🔴 Health server stopped")


def full_regeneration_thread(mysql_settings, app_settings):
    global USER_FUNC, REGENERATION_CONTROLLER

    db_name = app_settings['db_name']
    tables_name = app_settings['init_tables']
    full_regeneration_batch_len = int(app_settings['full_regeneration_batch_len'])

    conn = pymysql.connect(**mysql_settings)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
    cursor.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT;")

    for table in tables_name:
        # requesting row count for each table in every thread
        # this is redundant but ensures we get maximum row count
        #   for high-load tables where each thread may have snapshots at different row counts
        # this data is needed for health server to report total row count
        cursor.execute(f"SELECT COUNT(*) as cnt FROM {db_name}.{table};")
        r = cursor.fetchall()
        count = r[0]['cnt']


        REGENERATION_CONTROLLER.put_rows_count(table, count)

    for table in tables_name:
        while True:
            current_id = REGENERATION_CONTROLLER.get_and_update_id(table, full_regeneration_batch_len)

            q = f"SELECT * FROM {db_name}.{table} WHERE id >= {current_id} and id < {current_id + full_regeneration_batch_len};"

            cursor.execute(q)
            result = cursor.fetchall()
            count = len(result)
            #logger.debug(f"Query: {q} count: {count}")
            if not count:
                break
            for r in result:
                USER_FUNC.process_event('insert', db_name, table, r)

            REGENERATION_CONTROLLER.add_parsed_count(table, count)

    conn.close()



def full_regeneration(cursor, mysql_settings, app_settings, binlog):
    global USER_FUNC, STAGE
    USER_FUNC.initiate_full_regeneration()
    STAGE = Stage.REGENERATION

    cursor.execute("SHOW MASTER STATUS;")
    r = cursor.fetchall()
    binlog.file = r[0][0]
    binlog.pos = r[0][1]

    assert binlog.save()

    threads = []

    for i in range(app_settings['full_regeneration_threads_count']):
        t = threading.Thread(target=full_regeneration_thread, args=(mysql_settings, app_settings,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    USER_FUNC.finished_full_regeneration()

def run():
    from config.config import MYSQL_SETTINGS, APP_SETTINGS
    global USER_FUNC, STAGE
    init(MYSQL_SETTINGS, APP_SETTINGS)

    health_thread = None
    conn = None
    try:
        conn = pymysql.connect(**MYSQL_SETTINGS)
        cursor = conn.cursor()

        preflight_check(cursor, MYSQL_SETTINGS, APP_SETTINGS)

        binlog = binlog_file(APP_SETTINGS['binlog_file'])

        health_thread = threading.Thread(target=health_server, daemon=True, args=(APP_SETTINGS['health_socket'], MYSQL_SETTINGS, ))
        health_thread.start()


        USER_FUNC.init()

        if not binlog.load():
            logger.debug(f"need full regeneration")
            full_regeneration(cursor, MYSQL_SETTINGS, APP_SETTINGS, binlog)
            logger.debug(f"regeneration - done")
        else:
            logger.debug(f"regenereation is not need, start from {str(binlog)}")


        start_binlog_consumer(MYSQL_SETTINGS, APP_SETTINGS, binlog)

    finally:
        if conn:
            conn.close()
        if health_thread:
            health_thread.join()

        USER_FUNC.tear_down()