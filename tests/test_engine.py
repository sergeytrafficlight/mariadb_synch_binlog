import threading

import pytest
import pymysql
import logging
import time
import os
import signal
import clickhouse_connect
from tests.conftest import start_engine, create_mariadb_db, create_clickhouse_db
from config.config_test import MYSQL_SETTINGS_ACTOR, APP_SETTINGS, MYSQL_SETTINGS
from plugins_test.plugin_test import statistic, CLICKHOUSE_SETTINGS_ACTOR
from src.tools import get_health_answer, get_gtid_diff, start, stop
from plugins_test.plugin_test import set_emulate_error

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


def generate_init_load(count):
    mysql_settings = MYSQL_SETTINGS_ACTOR
    mysql_settings['database'] = APP_SETTINGS['db_name']
    conn = pymysql.connect(
        **mysql_settings
    )
    cursor = conn.cursor()

    for i in range(count):
        name=f"name_{i}-init-load"
        #logger.debug(f"insert init {name}")
        cursor.execute(
            "INSERT INTO items (name, value) VALUES (%s, %s)",
            (name, i+1),
        )

        cursor.execute(
            "INSERT INTO items2 (name, value) VALUES (%s, %s)",
            (name, i+1),
        )

    cursor.execute("UPDATE items SET value = value + 100")

    conn.commit()

    cursor.execute("SHOW MASTER STATUS;")
    r = cursor.fetchall()
    #logger.debug(f"binlog pos after insert {r}")

    conn.close()

def generate_load(count, only_insert=False):
    mysql_settings = MYSQL_SETTINGS_ACTOR
    mysql_settings['database'] = APP_SETTINGS['db_name']
    conn = pymysql.connect(
        **mysql_settings
    )
    cursor = conn.cursor()

    for i in range(count):
        name=f"name_{i}-load"
        #logger.debug(f"insert load {name}")
        cursor.execute(
            "INSERT INTO items (name, value) VALUES (%s, %s)",
            (name, i+1),
        )
        cursor.execute(
            "INSERT INTO items2 (name, value) VALUES (%s, %s)",
            (name, i+1),
        )

    if not only_insert:
        cursor.execute("UPDATE items SET value = value + 100")
        cursor.execute("DELETE FROM items WHERE id % 2 = 0")
    conn.commit()
    conn.close()

def compare_db(equal = True):

    mysql_settings = MYSQL_SETTINGS_ACTOR
    mysql_settings['database'] = APP_SETTINGS['db_name']

    conn_mariadb = pymysql.connect(
        **MYSQL_SETTINGS_ACTOR,
    )
    cursor_mariadb = conn_mariadb.cursor()



    client_ch = clickhouse_connect.get_client(
        host=CLICKHOUSE_SETTINGS_ACTOR["host"],
        port=CLICKHOUSE_SETTINGS_ACTOR["port"],
        username=CLICKHOUSE_SETTINGS_ACTOR["user"],
        password=CLICKHOUSE_SETTINGS_ACTOR["password"],
        database=CLICKHOUSE_SETTINGS_ACTOR["database"],
    )

    cursor_mariadb.execute("SELECT COUNT(*), SUM(value) FROM items;")
    row_mariadb = cursor_mariadb.fetchall()
    mariadb_count = int(row_mariadb[0][0] or 0)
    mariadb_sum = int(row_mariadb[0][1] or 0)

    result = client_ch.query(
        '''
        SELECT
            COUNT(*) as count_rows,
            SUM(value) as sum_rows
        FROM
        (
            SELECT
                argMax(value, version) AS value,
                argMax(deleted, version) AS deleted
            FROM items
            GROUP BY id
        )       
        WHERE deleted = 0 
        '''
    )

    result = result.named_results()
    result = next(result)

    ch_count = int(result['count_rows'])
    ch_sum = int(result['sum_rows'])

    if equal:
        assert mariadb_count == ch_count
        assert mariadb_sum == ch_sum
    else:
        assert mariadb_count != ch_count
        assert mariadb_sum != ch_sum

    conn_mariadb.close()
    client_ch.close()

def clear_binlog_file():
    binlog_file_path = APP_SETTINGS['binlog_file']
    try:
        os.remove(binlog_file_path)
    except FileNotFoundError:
        pass

def _start():
    r = start(MYSQL_SETTINGS, APP_SETTINGS, as_thread=True)
    time.sleep(1)
    return r

def _stop(consumer):
    stop(consumer)
    assert not consumer.is_alive()



def test_start_stop():

    db_name = create_mariadb_db()
    db_clickhouse = create_clickhouse_db()

    clear_binlog_file()
    consumer = _start()
    _stop(consumer)


def test_init_load_insert():

    from plugins_test.plugin_test import statistic
    statistic.clear()

    db_name = create_mariadb_db()
    db_clickhouse = create_clickhouse_db()

    clear_binlog_file()

    compare_db()

    generate_init_load(100)
    consumer = _start()

    assert statistic.initiate_full_regeneration == 1
    assert statistic.process_event_insert == 100
    time.sleep(5)
    _stop(consumer)

    compare_db()


    statistic.clear()

    consumer = _start()
    assert statistic.process_event_insert == 0

    generate_load(1, True)

    time.sleep(1)
    assert statistic.process_event_insert == 1

    _stop(consumer)

    compare_db()

    statistic.clear()

    consumer = _start()
    assert statistic.process_event_insert == 0

    #generate error
    set_emulate_error(True)

    generate_load(1, True)

    _stop(consumer)
    compare_db(equal=False)

    set_emulate_error(False)

    consumer = _start()

    _stop(consumer)

    compare_db()



def test_engine_stresstest():

    global statistic
    statistic.clear()

    clear_binlog_file()

    db_name = create_mariadb_db()
    db_clickhouse = create_clickhouse_db()

    consumer = _start()

    time.sleep(1)
    assert statistic.initiate_full_regeneration == 1

    compare_db()

    set_emulate_error(False)

    stress_test_duration_s = 60
    def _stress_load_thread(duration_s):
        start_time = time.time()

        while time.time() - start_time < duration_s:
            #print(f"generate load")
            generate_load(100, True)
            time.sleep(0.13)

    t_list = []

    for i in range(20):
        t = threading.Thread(target=_stress_load_thread, args=(stress_test_duration_s, ))
        t.start()
        t_list.append(t)

    start_time = time.time()

    _stop(consumer)

    print(f"Generating errors")
    while time.time() - start_time < stress_test_duration_s:
        statistic.clear()
        consumer = _start()
        set_emulate_error(True)
        time.sleep(1)
        _stop(consumer)
        set_emulate_error(False)

        assert statistic.initiate_full_regeneration == 0

        print(f'Emulate error, timer: {(time.time() - start_time)} of {stress_test_duration_s}')



    print(f"Waiting for threads")
    for t in t_list:
        t.join()


    #waiting for gtid lag == 0
    set_emulate_error(False)
    consumer = _start()

    print(f"Waiting for lag == 0")
    while True:
        time.sleep(1)
        answer = get_health_answer(APP_SETTINGS['health_socket'])
        lag = answer['gtid_diff']
        print(f"GTID lag: {lag}")
        if not lag:
            break

    _stop(consumer)
    compare_db()


def test_gtid_diff():

    r = get_gtid_diff("1-1-2236", "0-1-158,1-1-2236")
    assert r == 0
    r = get_gtid_diff("1-1-2236", "0-1-158,1-1-2237")
    assert r == 1
    r = get_gtid_diff("3-1-2236", "0-1-158,1-1-2237")
    assert r == 0
    r = get_gtid_diff("1-1-2236", "0-1-158,1-1-2235")
    assert r == 0
    r = get_gtid_diff("1-1-2236", None)
    assert r == 0
