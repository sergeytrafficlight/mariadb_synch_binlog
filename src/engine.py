import signal
import os
import time
import pymysql
from datetime import timedelta
import logging
import socket
import threading
import importlib
import sys
import json
from enum import Enum
import traceback
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent
from pymysqlreplication.event import MariadbGtidEvent, XidEvent, QueryEvent

from .tools import binlog_file, plugin_wrapper, regeneration_threads_controller, get_binlog_diff, get_binlog_from_db, insert_buffer
from .synch_storage import synch_storage

logging.getLogger("pymysqlreplication").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
logger.setLevel(logging.CRITICAL)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s:%(lineno)d  - %(message)s"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

def thread_exception_handler(args):
    print(f"❌ Thread {args.thread.name} crashed:", args.exc_value)

    traceback.print_exception(
        args.exc_type,
        args.exc_value,
        args.exc_traceback
    )
    
    sys.exit(1)

threading.excepthook = thread_exception_handler

class Stage(Enum):
    INIT = 'INIT'
    REGENERATION = 'REGENERATION'
    REGENERATION_PARSED_DONE = 'REGENERATION_PARSED_DONE'
    REGENERATION_DUMP_DONE = 'REGENERATION_DUMP_DONE'
    SYNCH = 'SYNCH'

USER_FUNC = None
STOP = None
LAST_SIGINT = None
FORCE_EXIT_WINDOW = None
STAGE = None
REGENERATION_CONTROLLER = None
#binlog for all trx
PARSED_BINLOG_TOTAL = None
#binlog only for my tables
PARSED_BINLOG_MY = None
GLOBAL_LOCK = threading.Lock()
SYNCH_STORAGE = None

def init(MYSQL_SETTINGS, APP_SETTINGS):
    global USER_FUNC, STOP, LAST_SIGINT, FORCE_EXIT_WINDOW, STAGE, REGENERATION_CONTROLLER, PARSED_BINLOG, PARSED_BINLOG_MY
    USER_FUNC = plugin_wrapper(APP_SETTINGS['handle_events_plugin'])
    STOP = False
    LAST_SIGINT = 0
    FORCE_EXIT_WINDOW = 1.5
    STAGE = Stage.INIT
    REGENERATION_CONTROLLER = regeneration_threads_controller(APP_SETTINGS['full_regeneration_threads_count'])
    PARSED_BINLOG = None
    PARSED_BINLOG_MY = None

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

def save_binlog_position(binlog):
    logger.info(f"save binlog {binlog}")
    if binlog:
        assert binlog.save()


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

def preflight_check_ex(cursor, mysql_settings, app_settings):
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
            #errors.append(f"binlog_gtid_index {vars.get('binlog_gtid_index')} != ON")
            pass

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
            server_id=999999,
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
            return

    def check_tables(cursor, db_name, tables):

        q = f"SHOW TABLES FROM {db_name};"
        cursor.execute(q)

        r = cursor.fetchall()
        existing_tables = []
        for t in r:
            existing_tables.append(t[0])

        errors = []
        for t in tables:
            if t not in existing_tables:
                errors.append(t)

        if len(errors):
            raise RuntimeError(f"Can't find tables: {errors} | db_name {db_name}\nquery: {q}\nfetch result: {r}")

    check_grants(cursor)
    check_variables(cursor)
    assert_readonly(cursor)
    probe_binlog(mysql_settings)
    if len(app_settings.get('scan_tables', [])):
        check_tables(cursor, app_settings['db_name'], app_settings['scan_tables'])


def start_binlog_consumer(mysql_settings, app_settings, binlog):
    global USER_FUNC, GLOBAL_LOCK, STAGE, PARSED_BINLOG_TOTAL, PARSED_BINLOG_MY, SYNCH_STORAGE
    from .tools import check_binlog_in_range

    if not check_binlog_in_range(mysql_settings, binlog):
        raise ValueError(f"Binlog {binlog} is out of range")


    USER_FUNC.initiate_synch_mode()
    STAGE = Stage.SYNCH

    binlog_stream = BinLogStreamReader(
        connection_settings=mysql_settings,
        server_id=app_settings['unique_consumer_server_id'],          # уникальный server_id для consumer
        blocking=False,             # ждём новые события
        resume_stream=True,        # продолжим с последней позиции, если указана
        only_events=[
            WriteRowsEvent,
            UpdateRowsEvent,
            DeleteRowsEvent,
            XidEvent,
            QueryEvent,
        ],
        only_schemas=[app_settings['db_name']],
        only_tables=app_settings['scan_tables'],
        freeze_schema=True,
        log_file=binlog.file,
        log_pos=binlog.pos,
    )

    logger.info(f"🚀 Binlog consumer started from {binlog}. Synch with [{app_settings['db_name']}] . Waiting for events...")

    try:
        while not STOP:
            for event in binlog_stream:
                if STOP:
                    break

                if isinstance(event, XidEvent):

                    binlog.pos = event.packet.log_pos
                    binlog.file = binlog_stream.log_file
                    PARSED_BINLOG_TOTAL = binlog.copy()
                    SYNCH_STORAGE.put_binlog(binlog.copy())

                elif event.schema != app_settings['db_name']:
                    pass
                elif event.table not in app_settings['scan_tables']:
                    pass
                elif isinstance(event, (WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent)):
                    PARSED_BINLOG_MY = binlog.copy()
                    for row in event.rows:
                        if STOP:
                            break
                        if isinstance(event, WriteRowsEvent):
                            SYNCH_STORAGE.put_event(event_type='insert', table=event.table, event=row['values'])
                        elif isinstance(event, UpdateRowsEvent):
                            SYNCH_STORAGE.put_event(event_type='update', table=event.table, event=row)
                        elif isinstance(event, DeleteRowsEvent):
                            SYNCH_STORAGE.put_event(event_type='delete', table=event.table, event=row)

            time.sleep(0.2)
    except Exception as e:
        logger.exception(f"Consumer exception: {e}")
    finally:
        binlog_stream.close()
        return


def health_server(socket_path, mysql_settings, app_settings):

    global STOP, GLOBAL_LOCK, REGENERATION_CONTROLLER, STAGE, PARSED_BINLOG_TOTAL, PARSED_BINLOG_MY
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
                binlog_db = get_binlog_from_db(mysql_settings, app_settings)
                binlog_saved = binlog_file(file_path=app_settings['binlog_file'])
                if not binlog_saved.load():
                    binlog_saved = None

                with GLOBAL_LOCK:
                    init_rows_total, init_rows_parsed, estimate = REGENERATION_CONTROLLER.statistic()
                    if estimate:
                        human_estimate = str(timedelta(seconds=int(estimate)))
                    else:
                        human_estimate = ''

                    response = {
                        "status": "ok",
                        "stage": str(STAGE),
                        "init_rows_total": init_rows_total,
                        "init_rows_parsed": init_rows_parsed,
                        "regeneration_estimate_s": int(estimate) if estimate else '',
                        "regeneration_human_estimate_s": human_estimate,
                        "binlog_server_current": str(binlog_db),
                        "binlog_server_parsed": str(PARSED_BINLOG_TOTAL),
                        "binlog_server_app": str(PARSED_BINLOG_MY),
                        "consumer_binlog": str(binlog_saved),
                        "binlog_parsed_diff": get_binlog_diff(mysql_settings, PARSED_BINLOG_TOTAL, binlog_db),
                        "binlog_diff": get_binlog_diff(mysql_settings, binlog_saved, binlog_db),
                        "error": '',
                    }
                    try:
                        conn.sendall((json.dumps(response) + "\n").encode())
                    except socket.timeout:
                        logger.warning("Send timeout")
                    except (BrokenPipeError, ConnectionError) as e:
                        logger.warning(f"Client disconnected: {e}")
    except Exception as e:
        logger.critical(f"Health server exception: {e}")
    finally:
        server.close()
        if os.path.exists(socket_path):
            os.remove(socket_path)
        logger.info("🔴 Health server stopped")
        return



def full_regeneration_thread(mysql_settings, app_settings):
    global USER_FUNC, REGENERATION_CONTROLLER, SYNCH_STORAGE, STAGE

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
        q = f"SELECT COUNT(*) as cnt, MIN(id) as min_id, MAX(id) as max_id FROM {db_name}.{table};"

        cursor.execute(q)
        r = cursor.fetchall()
        count = r[0]['cnt']
        min_id = r[0]['min_id']
        max_id = r[0]['max_id']
        logger.debug(f"{q} count: {count} min_id: {min_id} max_id: {max_id}")

        if STAGE == Stage.INIT:
            STAGE = Stage.REGENERATION
        REGENERATION_CONTROLLER.put_rows_count(table, count, min_id, max_id)

    REGENERATION_CONTROLLER.barrier.wait()

    for table in tables_name:
        while True:
            current_id = REGENERATION_CONTROLLER.get_and_update_id(table, full_regeneration_batch_len)

            q = f"SELECT * FROM {db_name}.{table} WHERE id >= {current_id} and id < {current_id + full_regeneration_batch_len};"


            cursor.execute(q)
            result = cursor.fetchall()
            count = len(result)
            logger.debug(f"Query: {q} count: {count}")
            if not count:
                if REGENERATION_CONTROLLER.is_end(table):
                    break
                else:
                    continue
            for r in result:
                #USER_FUNC.process_event('insert', db_name, table, r)
                SYNCH_STORAGE.put_event(event_type='insert', table=table, event=r)

    conn.close()



def full_regeneration(mysql_settings, app_settings):
    global USER_FUNC, STAGE, PARSED_BINLOG_TOTAL, PARSED_BINLOG_MY, SYNCH_STORAGE, STOP, REGENERATION_CONTROLLER
    USER_FUNC.initiate_full_regeneration()

    binlog = get_binlog_from_db(mysql_settings, app_settings)

    threads = []

    for i in range(app_settings['full_regeneration_threads_count']):
        t = threading.Thread(target=full_regeneration_thread, args=(mysql_settings, app_settings,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    STAGE = Stage.REGENERATION_PARSED_DONE
    # waiting for stop full regeneration

    while not STOP and STAGE != Stage.REGENERATION_DUMP_DONE:
        time.sleep(1)

    USER_FUNC.finished_full_regeneration()

    PARSED_BINLOG_TOTAL = binlog.copy()
    PARSED_BINLOG_MY = binlog.copy()

    save_binlog_position(binlog)

    return binlog

def worker_thread(buffer_data, insert_storage):
    global STOP
    while not STOP:
        event = buffer_data.get_event()
        if event is None:
            return

        result = USER_FUNC.process_event(event.event_type, event.table, event.event)

        assert result is not None, f"Unexpected None for result"

        for r in result:
            insert_storage.push(r.table_name, r.columns, r.values)


def run_workers_thread(app_settings):

    global STOP, SYNCH_STORAGE, STAGE, USER_FUNC
    timeout = app_settings['clickhouse_dropdown_sleep']


    logger.info(f"workers threads")

    while not STOP:
        time.sleep(timeout)
        sync_mode = (STAGE == Stage.SYNCH)
        logger.info(f"run threads, sync mode: {sync_mode}")

        buffer_data = SYNCH_STORAGE.get_buffer(expecting_binlog=sync_mode)
        if buffer_data is None:
            logger.info(f"buffer data is empty")
            continue

        if not buffer_data.len():
            if STAGE == Stage.REGENERATION_PARSED_DONE:
                STAGE = Stage.REGENERATION_DUMP_DONE

            if buffer_data.binlog:
                save_binlog_position(buffer_data.binlog)
            logger.info(f'skip due stage: {STAGE}')
            continue

        insert_storage = insert_buffer()

        if STAGE == Stage.SYNCH:
            USER_FUNC.initiate_dropdown_workers()

        logger.info(f"launch worker threads")

        threads = []
        for i in range(app_settings['full_regeneration_threads_count']):
            t = threading.Thread(target=worker_thread, args=(buffer_data, insert_storage))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        logger.info(f"workers done")

        while True:
            rows = insert_storage.get_similar_pack_clear()
            if rows is None:
                logger.info("rows len is None")
                break

            if len(rows):
                columns = rows[0].keys
                values = [p.values for p in rows]
                try:
                    USER_FUNC.dump_values(rows[0].table_name, columns, values)
                    print(f"stage: {STAGE} table: {rows[0].table_name} len: {len(values)}")
                    if STAGE in [Stage.REGENERATION, Stage.REGENERATION_PARSED_DONE]:
                        REGENERATION_CONTROLLER.add_parsed_count(len(values))
                except Exception as e:
                    logger.exception(e)
                    #to notify all other threads to stop
                    STOP = True
                    SYNCH_STORAGE.get_buffer(expecting_binlog=sync_mode)
                    return
            else:
                pass

        if sync_mode:
            assert buffer_data.binlog is not None, f"Binlog can't be None here"

        if buffer_data.binlog:
            save_binlog_position(buffer_data.binlog)


def run(MYSQL_SETTINGS, APP_SETTINGS):

    global USER_FUNC, STAGE, STOP, SYNCH_STORAGE

    init(MYSQL_SETTINGS, APP_SETTINGS)
    SYNCH_STORAGE = synch_storage(max_len = APP_SETTINGS['clickhouse_max_batch_len'])

    health_thread = None
    workers_thread = None

    try:
        conn = pymysql.connect(**MYSQL_SETTINGS)
        cursor = conn.cursor()

        preflight_check_ex(cursor, MYSQL_SETTINGS, APP_SETTINGS)

        if conn:
            conn.close()


        binlog = binlog_file(APP_SETTINGS['binlog_file'])

        health_thread = threading.Thread(target=health_server, daemon=True, args=(APP_SETTINGS['health_socket'], MYSQL_SETTINGS, APP_SETTINGS,))
        health_thread.start()

        USER_FUNC.init()

        workers_thread = threading.Thread(target=run_workers_thread, daemon=True, args=(APP_SETTINGS,))
        workers_thread.start()

        if not binlog.load():
            logger.debug(f"need full regeneration")
            binlog = full_regeneration(MYSQL_SETTINGS, APP_SETTINGS)
            logger.debug(f"regeneration - done")
        else:
            logger.debug(f"regenereation is not need, start from {str(binlog)}")


        start_binlog_consumer(MYSQL_SETTINGS, APP_SETTINGS, binlog)

    except Exception as e:
        logger.exception(f"Exception: {e}")
        logger.traceback(e)
        exit(-1)

    finally:
        STOP = True
        if health_thread:
            health_thread.join()
        if workers_thread:
            workers_thread.join()

        USER_FUNC.tear_down()
        return 0


def stop():
    global STOP
    STOP = True
